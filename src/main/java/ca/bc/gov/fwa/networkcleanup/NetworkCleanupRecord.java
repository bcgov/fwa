package ca.bc.gov.fwa.networkcleanup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jeometry.common.number.Doubles;

import ca.bc.gov.fwa.convert.FwaConstants;

import com.revolsys.datatype.DataTypes;
import com.revolsys.geometry.graph.BinaryRoutePath;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Lineal;
import com.revolsys.record.AbstractRecord;
import com.revolsys.record.Record;
import com.revolsys.record.RecordState;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.util.Debug;

public class NetworkCleanupRecord extends AbstractRecord implements FwaConstants {

  public static int maxRouteCount = 0;

  public static List<String> FWA_FIELD_NAMES = Arrays.asList(//
    LINEAR_FEATURE_ID, //
    BLUE_LINE_KEY, //
    FWA_WATERSHED_CODE, //
    GNIS_NAME, //
    LOCAL_WATERSHED_CODE, //
    LENGTH_METRE, //
    DOWNSTREAM_LENGTH, //
    UPSTREAM_LENGTH, //
    // ROUTES,
    GEOMETRY);

  public static final RecordDefinition RECORD_DEFINITION = new RecordDefinitionBuilder(
    "/fwa_network_cleanup") //
      .addField("id", DataTypes.LONG) //
      .addField("blueLineKey", DataTypes.INT) //
      .addField("name", DataTypes.STRING) //
      .addField("watershedCode", DataTypes.STRING) //
      .addField("localWatershedCode", DataTypes.STRING) //
      .addField("length", DataTypes.DOUBLE) //
      .addField("downstreamLength", DataTypes.DOUBLE) //
      .addField("upstreamLength", DataTypes.DOUBLE) //
      .addField("routes", DataTypes.OBJECT) //
      .addField("ring", DataTypes.LINE_STRING) //
      .setGeometryFactory(GEOMETRY_FACTORY) //
      .getRecordDefinition();

  public static int maxRouteLength;

  private final int id;

  private final int blueLineKey;

  private final String watershedCode;

  private String localWatershedCode;

  private final double length;

  private double downstreamLength;

  private double upstreamLength;

  private RecordState state;

  private final LineString line;

  private final Lineal lineal;

  private List<BinaryRoutePath> routePaths;// = new ArrayList<>();

  private List<byte[]> routes;// = new ArrayList<>();

  private boolean processed = false;

  private final String name;

  public NetworkCleanupRecord(final Record record) {
    this.id = record.getInteger(LINEAR_FEATURE_ID);
    this.watershedCode = intern(record, FWA_WATERSHED_CODE);
    this.blueLineKey = record.getInteger(BLUE_LINE_KEY);
    this.name = intern(record, GNIS_NAME);
    this.localWatershedCode = intern(record, LOCAL_WATERSHED_CODE);
    this.length = Doubles.makePrecise(1000, record.getDouble(LENGTH_METRE));
    this.downstreamLength = record.getDouble(DOWNSTREAM_LENGTH, 0);
    this.upstreamLength = record.getDouble(UPSTREAM_LENGTH, 0);
    // final List<Blob> routes = record.getValue(ROUTES, Collections.emptyList());
    // try {
    // for (final Blob blob : routes) {
    // this.routes.add(blob.getBytes(1, (int)blob.length()));
    // }
    // } catch (final SQLException e) {
    // throw Exceptions.wrap(e);
    // }
    this.lineal = record.getGeometry();
    this.line = this.lineal.getLineString(0);
    this.state = RecordState.PERSISTED;
  }

  public boolean addRoute(final BinaryRoutePath route) {
    if (this.routePaths == null) {
      this.routePaths = new ArrayList<>();
    }
    for (final BinaryRoutePath route2 : this.routePaths) {
      if (route.startsWith(route2)) {
        return false;
      }
    }
    this.routePaths.add(route);
    if (!route.toString().equals(new BinaryRoutePath(route.toBytes()).toString())) {
      Debug.noOp();
    }
    maxRouteCount = Math.max(maxRouteCount, this.routePaths.size());
    maxRouteLength = Math.max(maxRouteLength, route.getEdgeCount());
    return true;
  }

  public int getBlueLineKey() {
    return this.blueLineKey;
  }

  public double getDownstreamLength() {
    return this.downstreamLength;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Geometry> T getGeometry() {
    return (T)this.line;
  }

  public int getId() {
    return this.id;
  }

  public double getLength() {
    return this.length;
  }

  public LineString getLine() {
    return this.line;
  }

  public Lineal getLineal() {
    return this.lineal;
  }

  public String getLocalWatershedCode() {
    return this.localWatershedCode;
  }

  public String getName() {
    return this.name;
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return RECORD_DEFINITION;
  }

  public List<BinaryRoutePath> getRoutePaths() {
    if (this.routePaths == null) {
      return Collections.emptyList();
    } else {
      return this.routePaths;
    }
  }

  public List<byte[]> getRoutes() {
    if (this.routes == null) {
      return Collections.emptyList();
    } else {
      return this.routes;
    }
  }

  @Override
  public RecordState getState() {
    return this.state;
  }

  public double getUpstreamLength() {
    return this.upstreamLength;
  }

  public String getWatershedCode() {
    return this.watershedCode;
  }

  public String intern(final Record record, final String fieldName) {
    String text = record.getString(fieldName);
    if (text != null) {
      text = text.intern();
    }
    return text;
  }

  public boolean isMultiLine() {
    return this.lineal.isGeometryCollection();
  }

  public boolean isProcessed() {
    return this.processed;
  }

  public boolean isRouteModified() {
    boolean updateRoutes = false;

    final List<BinaryRoutePath> routePaths = getRoutePaths();
    final int newRouteCount = routePaths.size();
    final List<byte[]> routes = getRoutes();
    if (routes.size() != newRouteCount) {
      updateRoutes = true;
    } else if (!routePaths.isEmpty()) {
      routePaths.sort(BinaryRoutePath.COMPARATOR);
      for (int i = 0; i < newRouteCount; i++) {
        final BinaryRoutePath route = routePaths.get(i);
        final byte[] oldBytes = routes.get(i);
        if (!route.equalsBytes(oldBytes)) {
          updateRoutes = true;
          break;
        }
      }
    }
    return updateRoutes;
  }

  public void resetLengths() {
    this.downstreamLength = 0;
    this.upstreamLength = 0;
  }

  public void setDownstreamLength(final double downstreamLength) {
    if (this.downstreamLength != downstreamLength) {
      this.state = RecordState.MODIFIED;
      this.downstreamLength = downstreamLength;
    }
  }

  public void setLocalWatershedCode(final String localWatershedCode) {
    this.localWatershedCode = localWatershedCode;
  }

  public void setProcessed(final boolean processed) {
    this.processed = processed;
  }

  public void setUpstreamLength(final double upstreamLength) {
    if (this.upstreamLength != upstreamLength) {
      this.state = RecordState.MODIFIED;
      this.upstreamLength = upstreamLength;
    }
  }
}
