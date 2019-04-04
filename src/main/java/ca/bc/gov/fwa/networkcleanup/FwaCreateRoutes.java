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

import com.revolsys.geometry.graph.BinaryRoutePath;
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

public class FwaCreateRoutes implements FwaConstants {

  private static final String ROUTE_UPDATE_SQL = "UPDATE FWA.FWA_RIVER_NETWORK SET " //
    + "ROUTES = ? "//
    + "WHERE LINEAR_FEATURE_ID = ?";

  private static final int LOG_STEP = 100000;

  private static final String UPDATE_SQL = "UPDATE FWA.FWA_RIVER_NETWORK SET " //
    + "DOWNSTREAM_LENGTH = ?, "//
    + "UPSTREAM_LENGTH = ? "//
    + "WHERE LINEAR_FEATURE_ID = ?";

  public static void main(final String[] args) {
    new FwaCreateRoutes().run();
  }

  private final JdbcRecordStore recordStore = (JdbcRecordStore)FwaController.getFwaRecordStore();

  private int count = 0;

  private int updateCount = 0;

  private final Set<Node<Record>> processedNodes = new HashSet<>();

  private final Set<Integer> processedBlueLineKeys = new HashSet<>();

  // private final Set<Node<Record>> processedNodes = new HashSet<>();

  private boolean addEdge(final boolean add, final int currentBlueLineKey, final int blueLineKey) {
    return add
      && (this.processedBlueLineKeys.add(blueLineKey) || currentBlueLineKey == blueLineKey);
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

  private void routePathsProcess(final Node<Record> node) {
    if (node.getInEdgeCount() == 0) {
      logCount("Outlet");
      this.processedNodes.clear();
      this.processedBlueLineKeys.clear();
      final int x = (int)Math.round(node.getX() * 1000);
      final int y = (int)Math.round(node.getY() * 1000);
      final int currentBlueLineKey = -1;
      final BinaryRoutePath route = new BinaryRoutePath(x, y);
      routePathsProcessEdges(route, currentBlueLineKey, node);
    }
  }

  private void routePathsProcess(final RecordGraph graph) {
    graph.forEachNode(this::routePathsProcess);
    System.out.println(NetworkCleanupRecord.maxRouteCount);
    System.out.println(NetworkCleanupRecord.maxRouteLength);
    logTotal("Outlet");
  }

  private void routePathsProcessEdges(final BinaryRoutePath route, final int currentBlueLineKey,
    final Node<Record> fromNode) {
    final int outEdgeCount = fromNode.getOutEdgeCount();
    if (outEdgeCount == 0) {
      Debug.noOp();
    } else if (this.processedNodes.add(fromNode)) {
      final Edge<Record> edge1 = fromNode.getOutEdge(0);
      final NetworkCleanupRecord record1 = edge1.getEdgeObject();
      final boolean add1 = record1.addRoute(route);
      final int blueLineKey1 = record1.getBlueLineKey();
      if (outEdgeCount == 1) {
        if (addEdge(add1, currentBlueLineKey, blueLineKey1)) {
          final BinaryRoutePath nextRoute = route.appendEdge(false);
          final Node<Record> toNode = edge1.getToNode();
          routePathsProcessEdges(nextRoute, blueLineKey1, toNode);
        }
      } else if (outEdgeCount == 2) {
        final Edge<Record> edge2 = fromNode.getOutEdge(1);
        final NetworkCleanupRecord record2 = edge2.getEdgeObject();
        final boolean add2 = record2.addRoute(route);
        final int blueLineKey2 = record2.getBlueLineKey();

        boolean edge1Primary = true;
        if (currentBlueLineKey < 0) {
          final String watershedCode1 = record1.getWatershedCode();
          final String watershedCode2 = record2.getWatershedCode();
          int compare = watershedCode1.compareTo(watershedCode2);
          if (compare == 0) {
            final String localWatershedCode1 = record1.getLocalWatershedCode();
            final String localWatershedCode2 = record2.getLocalWatershedCode();
            compare = localWatershedCode1.compareTo(localWatershedCode2);
          }
          edge1Primary = compare <= 0;
        } else if (blueLineKey1 == currentBlueLineKey) {
          if (blueLineKey2 == currentBlueLineKey) {
            Debug.noOp();
          } else {
            edge1Primary = true;
          }
        } else if (blueLineKey2 == currentBlueLineKey) {
          edge1Primary = false;
        } else {
          Debug.noOp();
        }
        if (add1 != add2) {
          Debug.noOp();
        }
        final BinaryRoutePath nextRoute1 = route.appendEdge(!edge1Primary);
        final Node<Record> toNode1 = edge1.getToNode();

        final BinaryRoutePath nextRoute2 = route.appendEdge(edge1Primary);
        final Node<Record> toNode2 = edge2.getToNode();

        // Always process primary first
        if (edge1Primary) {
          if (addEdge(add1, currentBlueLineKey, blueLineKey1)) {
            routePathsProcessEdges(nextRoute1, blueLineKey1, toNode1);
          }
          if (addEdge(add2, currentBlueLineKey, blueLineKey2)) {
            routePathsProcessEdges(nextRoute2, blueLineKey2, toNode2);
          }
        } else {
          if (addEdge(add2, currentBlueLineKey, blueLineKey2)) {
            routePathsProcessEdges(nextRoute2, blueLineKey2, toNode2);
          }
          if (addEdge(add1, currentBlueLineKey, blueLineKey1)) {
            routePathsProcessEdges(nextRoute1, blueLineKey1, toNode1);
          }
        }
      } else {
        throw new RuntimeException("Cannot have more than 2 edges");
      }
      this.processedNodes.remove(fromNode);
    } else {
      System.err.println(fromNode);
    }
  }

  private void run() {
    final RecordGraph graph = newGraph();
    routePathsProcess(graph);
    // updateRecords(graph);
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
