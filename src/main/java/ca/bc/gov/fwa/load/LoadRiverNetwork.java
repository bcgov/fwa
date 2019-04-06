package ca.bc.gov.fwa.load;

import java.util.concurrent.atomic.AtomicInteger;

import org.jeometry.common.date.Dates;
import org.jeometry.common.io.PathName;

import ca.bc.gov.fwa.FwaController;
import ca.bc.gov.fwa.convert.FwaConstants;

import com.revolsys.geometry.model.LineString;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.store.Buffer;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.record.schema.RecordStoreSchema;
import com.revolsys.transaction.Transaction;

public class LoadRiverNetwork implements FwaConstants {

  public static void main(final String[] args) {
    new LoadRiverNetwork().run();
  }

  public void cleanWatershedCode(final Record record) {
    cleanWatershedCode(record, FWA_WATERSHED_CODE);
    cleanWatershedCode(record, LOCAL_WATERSHED_CODE);
  }

  private void cleanWatershedCode(final Record record, final String fieldName) {
    String watershedCode = record.getString(fieldName);
    watershedCode = watershedCode.replaceAll("(-000000)+$", "");
    record.put(fieldName, watershedCode);
  }

  private RecordStore getStreamNetworkRecordStore() {
    return FwaController.getFileRecordStore("/Data/FWA/FWA_STREAM_NETWORKS_SP.gdb");
  }

  private void run() {
    long startTime = System.currentTimeMillis();
    final AtomicInteger count = new AtomicInteger(0);
    try (
      RecordStore fgdbRecordStore = getStreamNetworkRecordStore();
      final RecordStore jdbcRecordStore = FwaController.getFwaRecordStore();) {
      jdbcRecordStore.initialize();
      final Channel<PathName> names = new Channel<>(new Buffer<>());
      names.writeConnect();
      final RecordStoreSchema rootSchema = fgdbRecordStore.getRootSchema();
      for (final PathName pathName : rootSchema.getTypePaths()) {
        if (!pathName.getName().startsWith("_")) {
          names.write(pathName);
        }
        names.writeDisconnect();
      }
      try (
        Transaction transaction = jdbcRecordStore.newTransaction()) {
        jdbcRecordStore.deleteRecords(new Query(FWA_RIVER_NETWORK));
      }
      final ProcessNetwork network = new ProcessNetwork();
      for (int i = 0; i < 4; i++) {
        network.addProcess(() -> {
          while (!names.isClosed()) {
            final PathName pathName = names.read();
            try (
              RecordReader reader = fgdbRecordStore.getRecords(pathName);
              Transaction transaction = jdbcRecordStore.newTransaction();
              RecordWriter writer = jdbcRecordStore.newRecordWriter(FWA_RIVER_NETWORK)) {
              for (final Record record : reader) {
                if (count.incrementAndGet() % 50000 == 0) {
                  System.out.println(count);
                }
                String watershedCode = record.getString(FWA_WATERSHED_CODE);
                if (watershedCode == null) {
                  watershedCode = "";
                } else {
                  if (watershedCode.startsWith("999")) {
                    watershedCode = "999";
                  } else {
                    watershedCode = watershedCode.replaceAll("(-0+)+$\\s*", "");
                  }
                  record.setValue(FWA_WATERSHED_CODE, watershedCode.intern());
                }

                String localWatershedCode = record.getString(LOCAL_WATERSHED_CODE);
                if (localWatershedCode != null && !"<Null>".equals(localWatershedCode)) {
                  if (localWatershedCode.startsWith("999")) {
                    localWatershedCode = "999";
                  } else {
                    localWatershedCode = localWatershedCode.replaceAll("(-0+)+$\\s*", "");
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

                LineString line = record.getGeometry();
                line = line.convertAxisCount(2);
                record.setGeometryValue(line);
                final Record newRecord = jdbcRecordStore.newRecord(FWA_RIVER_NETWORK, record);
                newRecord.setIdentifier(record.getIdentifier(LINEAR_FEATURE_ID));
                newRecord.setValue(DOWNSTREAM_LENGTH, 0);
                newRecord.setValue(UPSTREAM_LENGTH, 0);
                newRecord.setValue(BLUE_LINE_KEY_STREAM_ORDER, record, STREAM_ORDER);
                writer.write(newRecord);
              }
            }
          }
        });
      }
      network.startAndWait();
      startTime = Dates.printEllapsedTime("read: " + count, startTime);
    }
  }
}
