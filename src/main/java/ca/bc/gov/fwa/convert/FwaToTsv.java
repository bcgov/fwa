package ca.bc.gov.fwa.convert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jeometry.common.date.Dates;
import org.jeometry.common.io.PathName;

import ca.bc.gov.fwa.FwaController;

import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.LineString;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.record.schema.RecordStoreSchema;

public class FwaToTsv implements FwaConstants {

  static String FWA_STREAM_NETWORK_TSV = "/opt/dATA/FWA/FWA_STREAM_NETWORK.tsv";

  private static final GeometryFactory GEOMETRY_FACTORY = GeometryFactory.fixed2d(3005, 1000.0,
    1000.0);

  private static List<String> FIELD_NAMES = Arrays.asList("GEOMETRY", "LINEAR_FEATURE_ID",
    "EDGE_TYPE", BLUE_LINE_KEY, "WATERSHED_KEY", FWA_WATERSHED_CODE, LOCAL_WATERSHED_CODE,
    "WATERSHED_GROUP_CODE", "DOWNSTREAM_ROUTE_MEASURE", "LENGTH_METRE", "FEATURE_SOURCE", GNIS_ID,
    GNIS_NAME, "LEFT_RIGHT_TRIBUTARY", STREAM_ORDER, "STREAM_MAGNITUDE", "WATERBODY_KEY",
    "UPSTREAM_ROUTE_MEASURE");

  public static void main(final String[] args) {
    new FwaToTsv().run();
  }

  private RecordDefinition recordDefinition;

  private RecordWriter writer;

  private final Map<Integer, String> nameById = new TreeMap<>();

  private RecordStore getStreamNetworkRecordStore() {
    return FwaController.getFileRecordStore("/opt/data/FWA/FWA_STREAM_NETWORKS_SP.gdb");
  }

  private void readRecords() {
    long startTime = System.currentTimeMillis();
    final AtomicInteger count = new AtomicInteger();

    try (
      final RecordStore recordStore = getStreamNetworkRecordStore()) {
      final RecordStoreSchema rootSchema = recordStore.getRootSchema();
      for (final RecordDefinition recordDefinition : rootSchema.getRecordDefinitions()) {
        final PathName pathName = recordDefinition.getPathName();
        if (!pathName.getName().startsWith("_")) {
          try (
            RecordReader reader = recordStore.getRecords(pathName)) {
            if (this.recordDefinition == null) {
              final RecordDefinitionBuilder recordDefinitionBuilder = new RecordDefinitionBuilder();
              for (final String fieldName : FIELD_NAMES) {
                recordDefinitionBuilder.addField(recordDefinition.getField(fieldName));
              }
              recordDefinitionBuilder.setGeometryFactory(GEOMETRY_FACTORY);
              this.recordDefinition = recordDefinitionBuilder.getRecordDefinition();

              this.writer = RecordWriter.newRecordWriter(this.recordDefinition,
                FWA_STREAM_NETWORK_TSV);
            }
            for (final Record record : reader) {
              if (count.incrementAndGet() % 50000 == 0) {
                System.out.println(count);
              }
              String watershedCode = record.getString(FWA_WATERSHED_CODE);
              if (watershedCode == null) {
                watershedCode = "";
              } else {
                watershedCode = watershedCode.replaceAll("(-0+)+$\\s*", "");
                record.setValue(FWA_WATERSHED_CODE, watershedCode.intern());
              }

              String localWatershedCode = record.getString(LOCAL_WATERSHED_CODE);
              if (localWatershedCode != null) {
                localWatershedCode = localWatershedCode.replaceAll("(-0+)+\\s*$", "");
                if (localWatershedCode.endsWith("-000000")) {
                  System.err.println(localWatershedCode);
                }
                if (localWatershedCode.startsWith(watershedCode)) {
                  final int length = watershedCode.length();
                  if (length == localWatershedCode.length()) {
                    localWatershedCode = "";
                  } else {
                    localWatershedCode = localWatershedCode.substring(length + 1);
                  }
                }
                if (localWatershedCode.length() > 0) {
                  record.setValue(LOCAL_WATERSHED_CODE, localWatershedCode.intern());
                } else {
                  record.setValue(LOCAL_WATERSHED_CODE, null);
                }
              }
              final Integer nameId = record.getInteger(GNIS_ID);
              if (nameId != null) {
                final String name = record.getString(GNIS_NAME);
                this.nameById.put(nameId, name);
              }
              LineString line = record.getGeometry();
              line = line.convertAxisCount(2);
              record.setGeometryValue(line);
              this.writer.write(record);
            }
          }
        }
      }
    }
    this.writer.close();
    try (
      TsvWriter writer = Tsv.plainWriter("/opt/data/FWA/GNIS_NAMES.tsv")) {
      writer.write("ID", "NAME");
      for (final Entry<Integer, String> entry : this.nameById.entrySet()) {
        writer.write(entry.getKey(), entry.getValue());
      }
    }
    startTime = Dates.printEllapsedTime("read: " + count, startTime);
  }

  private void run() {
    readRecords();

  }
}
