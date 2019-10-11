package ca.bc.gov.fwa.convert;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.date.Dates;
import org.jeometry.common.io.PathName;

import com.revolsys.collection.map.IntHashMap;
import com.revolsys.collection.map.Maps;
import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.LineString;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.csv.CsvRecordWriter;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.util.count.LabelCountMap;
import com.revolsys.util.count.LabelCounters;

public class FwaTiles implements FwaConstants {
  private static final int COORDINATE_SYSTEM_ID = 3857;

  private static final GeometryFactory GEOMETRY_FACTORY = GeometryFactory
    .fixed2d(COORDINATE_SYSTEM_ID, 1000.0, 1000.0);

  private static final Map<Integer, Integer> SEGMENT_LENGTH_BY_TILE_SIZE = Maps
    .<Integer, Integer> buildHash() //
    .add(1000000, 10000) //
    .getMap();

  public static void main(final String[] args) {
    new FwaTiles().run();
  }

  private final IntHashMap<IntHashMap<IntHashMap<CsvRecordWriter>>> writersByTileSizeYAndX = new IntHashMap<>();

  private final RecordDefinition streamRecordDefinition = new RecordDefinitionBuilder(
    PathName.newPathName("/FWA_STREAM_TILE")) //
      .addField(LINEAR_FEATURE_ID, DataTypes.INT) //
      .addField(WATERSHED_CODE, DataTypes.STRING, 143) //
      .addField(LOCAL_WATERSHED_CODE, DataTypes.STRING, 143) //
      .addField(GeometryDataTypes.LINE_STRING) //
      .setGeometryFactory(GEOMETRY_FACTORY)//
      .getRecordDefinition();

  private final Path fwaPath = Paths
    .get("/opt/data/FWA/tiles/" + GEOMETRY_FACTORY.getHorizontalCoordinateSystemId());

  public void closeWriters() {
    for (final IntHashMap<IntHashMap<CsvRecordWriter>> writersByTileSize : this.writersByTileSizeYAndX
      .values()) {
      for (final IntHashMap<CsvRecordWriter> writersByTileX : writersByTileSize.values()) {
        for (final RecordWriter writer : writersByTileX.values()) {
          writer.close();
        }
      }
    }
    this.writersByTileSizeYAndX.clear();
  }

  public CsvRecordWriter getWriter(final int tileSize, final int tileX, final int tileY) {
    IntHashMap<IntHashMap<CsvRecordWriter>> writersByTileSize = this.writersByTileSizeYAndX
      .get(tileSize);
    if (writersByTileSize == null) {
      writersByTileSize = new IntHashMap<>();
      this.writersByTileSizeYAndX.put(tileSize, writersByTileSize);
    }
    IntHashMap<CsvRecordWriter> writersByTileY = writersByTileSize.get(tileX);
    if (writersByTileY == null) {
      writersByTileY = new IntHashMap<>();
      writersByTileSize.put(tileX, writersByTileY);
      final Path rowPath = this.fwaPath //
        .resolve(Integer.toString(tileSize)) //
        .resolve(Integer.toString(tileX));
      com.revolsys.io.file.Paths.createDirectories(rowPath);
    }

    CsvRecordWriter writer = writersByTileY.get(tileY);
    if (writer == null) {
      final Path tilePath = this.fwaPath //
        .resolve(Integer.toString(tileSize)) //
        .resolve(Integer.toString(tileX)) //
        .resolve(tileY + ".tsv");
      writer = (CsvRecordWriter)RecordWriter.newRecordWriter(this.streamRecordDefinition, tilePath);
      writersByTileY.put(tileY, writer);
    }
    return writer;
  }

  private Record newStream(final Record record, final int tileSize) {
    final Record stream = this.streamRecordDefinition.newRecord();
    stream.setValue(LINEAR_FEATURE_ID, record, LINEAR_FEATURE_ID);
    stream.setValue(WATERSHED_CODE, record, FWA_WATERSHED_CODE);
    stream.setValue(LOCAL_WATERSHED_CODE, record, LOCAL_WATERSHED_CODE);
    final LineString sourceLine = record.getGeometry();
    LineString line = sourceLine.convertGeometry(GEOMETRY_FACTORY);
    final int segmentLength = SEGMENT_LENGTH_BY_TILE_SIZE.getOrDefault(tileSize, 0);
    if (segmentLength > 0) {
      if (line.getVertexCount() > 2) {
        final double length = line.getLength();
        if (length < segmentLength) {
          final int lastVertexIndex = line.getLastVertexIndex();
          line = GEOMETRY_FACTORY.lineString(2, line.getX(0), line.getY(0),
            line.getX(lastVertexIndex), line.getY(lastVertexIndex));
        }
      }
    }
    stream.setGeometryValue(line);
    return stream;
  }

  public void pauseWriters() {
    for (final IntHashMap<IntHashMap<CsvRecordWriter>> writersByTileSize : this.writersByTileSizeYAndX
      .values()) {
      for (final IntHashMap<CsvRecordWriter> writersByTileX : writersByTileSize.values()) {
        for (final CsvRecordWriter writer : writersByTileX.values()) {
          writer.pause();
        }
      }
    }
  }

  private void readRecords() {
    long startTime = System.currentTimeMillis();
    final AtomicInteger count = new AtomicInteger();
    final LabelCounters counts = new LabelCountMap();
    try (
      RecordReader reader = RecordReader.newRecordReader(FwaToTsv.FWA_STREAM_NETWORK_TSV)) {
      for (final Record record : reader) {
        final int streamOrder = record.getInteger(STREAM_ORDER);
        counts.addCount(Integer.toString(streamOrder));
        final int i = count.incrementAndGet();
        if (i % 50000 == 0) {
          pauseWriters();
        }
        final List<Integer> tileSizes = new ArrayList<>();
        if (streamOrder >= 7) {
          tileSizes.add(1000000);
        }
        if (streamOrder >= 6) {
          tileSizes.add(500000);
        }
        if (streamOrder >= 4) {
          tileSizes.add(100000);
        }
        if (streamOrder >= 2) {
          tileSizes.add(10000);
        }
        tileSizes.add(5000);
        for (final int tileSize : tileSizes) {
          final Record tileRecord = newStream(record, tileSize);
          final LineString line = tileRecord.getGeometry();
          final BoundingBox boundingBox = line.getBoundingBox();
          final int minX = (int)Math.floor(boundingBox.getMinX() / tileSize) * tileSize;
          final int minY = (int)Math.floor(boundingBox.getMinY() / tileSize) * tileSize;
          final double maxX = boundingBox.getMaxX();
          final double maxY = boundingBox.getMaxY();
          for (int tileY = minY; tileY < maxY; tileY += tileSize) {
            for (int tileX = minX; tileX < maxX; tileX += tileSize) {
              final RecordWriter writer = getWriter(tileSize, tileX, tileY);
              writer.write(tileRecord);
            }
          }
        }
      }
    }
    closeWriters();
    System.out.println(counts.toTsv(STREAM_ORDER, "Counts"));
    startTime = Dates.printEllapsedTime("read: " + count, startTime);

  }

  private void run() {
    readRecords();

  }
}
