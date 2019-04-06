package ca.bc.gov.fwa.convert;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.jeometry.common.datatype.DataTypes;
import org.jeometry.common.date.Dates;
import org.jeometry.common.io.PathName;

import ca.bc.gov.fwa.FwaController;

import com.revolsys.geometry.graph.Edge;
import com.revolsys.geometry.graph.RecordGraph;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.LineString;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.io.FileUtil;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Transaction;

public class FwaRouteToNetwork {
  private static final String TO_DISTANCE = "TO_DISTANCE";

  private static final String FROM_DISTANCE = "FROM_DISTANCE";

  private static final GeometryFactory GEOMETRY_FACTORY = GeometryFactory.fixed2d(3005, 1000.0,
    1000.0);

  private static final String WATERSHED_CODE = "WATERSHED_CODE";

  private static final String ROUTE_ID = "ROUTE_ID";

  public static void main(final String[] args) {
    new FwaRouteToNetwork().run();
  }

  private RecordDefinition streamRecordDefinition;

  private Record newStream(final Record record) {
    final LineString line = record.getGeometry();

    final Record stream = this.streamRecordDefinition.newRecord();
    stream.setValue(ROUTE_ID, record, ROUTE_ID);
    stream.setValue(FROM_DISTANCE, 0);
    final double length = line.getLength();
    stream.setValue(TO_DISTANCE, Math.round(length * 1000.0) / 1000.0);
    stream.setValue(WATERSHED_CODE, record, FwaController.FWA_WATERSHED_CODE);
    stream.setGeometryValue(line);
    return stream;
  }

  private FileGdbRecordStore newStreamNetworkRecordStore() {

    final File file = new File("/Data/FWA/FWA_STREAM_NETWORK.gdb");
    FileUtil.deleteDirectory(file);
    final FileGdbRecordStore recordStore = FileGdbRecordStoreFactory.newRecordStore(file);

    recordStore.setCreateMissingRecordStore(true);
    recordStore.setCreateMissingTables(true);
    recordStore.initialize();

    final RecordDefinition streamRecordDefinition = new RecordDefinitionBuilder(
      PathName.newPathName("/FWA_STREAM_NETWORK")) //
        .addField(ROUTE_ID, DataTypes.INT) //
        .addField(FROM_DISTANCE, DataTypes.DOUBLE) //
        .addField(TO_DISTANCE, DataTypes.DOUBLE) //
        .addField(WATERSHED_CODE, DataTypes.STRING, 143) //
        .addField(GeometryDataTypes.LINE_STRING) //
        .setGeometryFactory(GEOMETRY_FACTORY)//
        .getRecordDefinition();
    this.streamRecordDefinition = recordStore.getRecordDefinition(streamRecordDefinition);

    return recordStore;
  }

  private RecordGraph readRecords() {
    long startTime = System.currentTimeMillis();
    final Query query = new Query(FwaController.FWA_ROUTES_SP);
    final AtomicInteger count = new AtomicInteger();
    final RecordGraph graph = new RecordGraph();

    try (
      final RecordStore recordStore = FwaController.getBcgwRecordStore();
      Transaction transaction = recordStore.newTransaction();
      RecordReader reader = recordStore.getRecords(query);) {
      for (final Record record : reader) {
        if (count.incrementAndGet() % 50000 == 0) {
          System.out.println(count);
        }
        final Record stream = newStream(record);
        graph.addEdge(stream);
      }
    }

    startTime = Dates.printEllapsedTime("read: " + count, startTime);
    return graph;
  }

  private void run() {
    try (
      final FileGdbRecordStore streamNetworkRecordStore = newStreamNetworkRecordStore()) {
      final RecordGraph graph = readRecords();

      splitEdges(graph);

      writeRecords(streamNetworkRecordStore, graph);
    }
  }

  private void splitEdges(final RecordGraph graph) {
    final AtomicInteger count = new AtomicInteger();
    final long startTime = System.currentTimeMillis();
    graph.forEachNode(node -> {
      if (count.incrementAndGet() % 50000 == 0) {
        System.out.println(count);
      }

      for (final Edge<Record> edge : graph.getEdges(node, 0)) {
        final Record record = edge.getObject();
        final double fromDistance = record.getDouble(FROM_DISTANCE);

        final List<Edge<Record>> splitEdges = edge.splitEdge(node);
        if (splitEdges.size() == 2) {
          final Edge<Record> edge1 = splitEdges.get(0);
          final double length1 = edge1.getLength();

          final double splitDistance = Math.round((fromDistance + length1) * 1000.0) / 1000.0;

          final Record record1 = edge1.getObject();
          record1.setValue(TO_DISTANCE, splitDistance);

          final Edge<Record> edge2 = splitEdges.get(1);
          final Record record2 = edge2.getObject();
          record2.setValue(FROM_DISTANCE, splitDistance);

        }
      }
    });

    Dates.printEllapsedTime("splitEdges: " + count, startTime);

  }

  private void writeRecords(final FileGdbRecordStore streamNetworkRecordStore,
    final RecordGraph graph) {
    final AtomicInteger count = new AtomicInteger();
    final long startTime = System.currentTimeMillis();
    try (
      RecordWriter writer = streamNetworkRecordStore.newRecordWriter()) {
      graph.forEachEdge(edge -> {
        final Record record = edge.getObject();
        if (count.incrementAndGet() % 50000 == 0) {
          System.out.println(count);
        }
        writer.write(record);
      });
    }
    Dates.printEllapsedTime("write: " + count, startTime);
  }

}
