package ca.bc.gov.fwa.networkcleanup;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jeometry.common.exception.Exceptions;
import org.jeometry.common.logging.Logs;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import ca.bc.gov.fwa.FwaController;
import ca.bc.gov.fwa.convert.FwaConstants;

import com.revolsys.geometry.graph.Edge;
import com.revolsys.geometry.graph.Node;
import com.revolsys.geometry.graph.RecordGraph;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Debug;
import com.revolsys.value.LongValue;

public class Fwa11SetUpstreamDownstreamLengths implements FwaConstants {

  private static final String ROUTE_UPDATE_SQL = "UPDATE FWA.FWA_RIVER_NETWORK SET " //
    + "ROUTES = ? "//
    + "WHERE LINEAR_FEATURE_ID = ?";

  private static final int LOG_STEP = 100000;

  private static final String UPDATE_SQL = "UPDATE FWA.FWA_RIVER_NETWORK SET " //
    + "DOWNSTREAM_LENGTH = ?, "//
    + "UPSTREAM_LENGTH = ? "//
    + "WHERE LINEAR_FEATURE_ID = ?";

  public static void main(final String[] args) {
    new Fwa11SetUpstreamDownstreamLengths().run();
  }

  private final JdbcRecordStore recordStore = (JdbcRecordStore)FwaController.getFwaRecordStore();

  private int count = 0;

  private int updateCount = 0;

  private final Set<Edge<Record>> processedEdges = new HashSet<>();

  private final LongValue total = new LongValue();

  private void addDownstreamLength(final Edge<Record> downstreanEdge) {
    if (this.processedEdges.add(downstreanEdge)) {
      final NetworkCleanupRecord downstreamRecord = (NetworkCleanupRecord)downstreanEdge
        .getObject();
      final long length = Math.round(downstreamRecord.getLength() * 1000);
      this.total.addValue(length);
      if (!downstreanEdge.isLoop()) {
        final Node<Record> fromNode = downstreanEdge.getFromNode();
        addDownstreamLength(fromNode);
      }
    }
  }

  private void addDownstreamLength(final Node<Record> node) {
    node.forEachInEdge(this::addDownstreamLength);
  }

  private void addUpstreamLength(final Edge<Record> upstreamEdge) {
    if (this.processedEdges.add(upstreamEdge)) {
      final NetworkCleanupRecord upstreamRecord = (NetworkCleanupRecord)upstreamEdge.getObject();
      final long length = Math.round(upstreamRecord.getLength() * 1000);
      this.total.addValue(length);
      if (!upstreamEdge.isLoop()) {
        final Node<Record> oppositeNode = upstreamEdge.getToNode();
        addUpstreamLength(oppositeNode);
      }
    }
  }

  private void addUpstreamLength(final Node<Record> node) {
    node.forEachOutEdge(this::addUpstreamLength);
  }

  private void logCount(final String message) {
    if (++this.count % LOG_STEP == 0) {
      System.out.println(message + "\t" + this.count);
    }
  }

  private void logTotal(final String message) {
    System.out.println(message + "\t" + this.count);
    this.count = 0;
  }

  private RecordGraph newGraph() {
    final String message = "Read";
    final RecordGraph graph = new RecordGraph();
    final Query query = new Query(FWA_RIVER_NETWORK) //
      .setFieldNames(NetworkCleanupRecord.FWA_FIELD_NAMES) //
    // .setWhereCondition(Q.like(FWA_WATERSHED_CODE, "300%")) //
    // .addOrderBy(FWA_WATERSHED_CODE) //
    // .addOrderBy(LOCAL_WATERSHED_CODE) //
    ;
    try (
      RecordReader reader = this.recordStore.getRecords(query)) {
      for (final Record record : reader) {
        logCount(message);
        final NetworkCleanupRecord networkCleanupRecord = new NetworkCleanupRecord(record);
        if ("999".equals(networkCleanupRecord.getWatershedCode())) {
          networkCleanupRecord.setDownstreamLength(0);
          networkCleanupRecord.setUpstreamLength(0);
          updateRecord(networkCleanupRecord);
        } else {
          graph.addEdge(networkCleanupRecord);
        }
      }
    }
    logTotal(message);
    return graph;
  }

  private void run() {
    final RecordGraph graph = newGraph();
    setDownstreamAndUpstreamLengths(graph);
    updateRecords(graph);
  }

  private void setDownstreamAndUpstreamLengths(final RecordGraph graph) {
    final String message = "Calculate Lengths";
    graph.forEachEdge(edge -> {
      logCount(message);

      final NetworkCleanupRecord record = (NetworkCleanupRecord)edge.getObject();

      this.processedEdges.clear();
      this.total.value = 0;
      final Node<Record> fromNode = edge.getFromNode();
      addDownstreamLength(fromNode);
      record.setDownstreamLength(this.total.value / 1000.0);

      this.processedEdges.clear();
      this.total.value = 0;
      final Node<Record> toNode = edge.getToNode();
      addUpstreamLength(toNode);
      record.setUpstreamLength(this.total.value / 1000.0);
    });
    logTotal(message);
  }

  private void updateRecord(final NetworkCleanupRecord record) {
    boolean updated = false;
    if (record.isModified()) {
      updated = true;
      final int id = record.getId();
      final double downstreamLength = record.getDownstreamLength();
      final double upstreamLength = record.getUpstreamLength();
      try (
        Transaction transaction = this.recordStore.newTransaction()) {
        JdbcUtils.executeUpdate(this.recordStore, UPDATE_SQL, downstreamLength, upstreamLength, id);
      } catch (final Exception e) {
        Debug.noOp();
      }
    }
    if (record.isRouteModified()) {
      updated = true;
      final int id = record.getId();
      final List<byte[]> routes = record.getRoutes();
      try (
        Transaction transaction = this.recordStore.newTransaction();
        final JdbcConnection connection = this.recordStore.getJdbcConnection()) {

        final LargeObjectManager lobManager = connection.unwrap(org.postgresql.PGConnection.class)
          .getLargeObjectAPI();

        final Object[] blobs = new Object[routes.size()];
        for (int i = 0; i < blobs.length; i++) {
          final byte[] bytes = routes.get(i);

          final long oid = lobManager.createLO(LargeObjectManager.READ | LargeObjectManager.WRITE);

          // Open the large object for writing
          final LargeObject blob = lobManager.open(oid, LargeObjectManager.WRITE);
          try {
            final OutputStream outputStream = blob.getOutputStream();
            outputStream.write(bytes);
          } finally {
            blob.close();
          }
          blobs[i] = oid;
        }
        final Array routeArray = connection.createArrayOf("oid", blobs);
        final PreparedStatement statement = connection.prepareStatement(ROUTE_UPDATE_SQL);
        try {
          statement.setArray(1, routeArray);
          statement.setInt(2, id);
          statement.executeUpdate();
        } finally {
          JdbcUtils.close(statement);
        }

      } catch (final SQLException | IOException e) {
        Logs.error(this, e);
        throw Exceptions.wrap(e);
      }
    }
    if (updated && ++this.updateCount % 50000 == 0) {
      System.out.println("Update records\t" + this.updateCount);
    }

  }

  private void updateRecords(final RecordGraph graph) {
    graph.forEachEdge(edge -> {
      final NetworkCleanupRecord record = (NetworkCleanupRecord)edge.getObject();
      updateRecord(record);
    });
    System.out.println("Update records\t" + this.updateCount);
  }

}
