package ca.bc.gov.fwa.convert;

import org.jeometry.common.io.PathName;

import com.revolsys.geometry.model.GeometryFactory;

public interface FwaConstants {
  static String BLUE_LINE_KEY = "BLUE_LINE_KEY";

  static String BLUE_LINE_KEY_STREAM_ORDER = "BLUE_LINE_KEY_STREAM_ORDER";

  static String DOWNSTREAM_LENGTH = "DOWNSTREAM_LENGTH";

  static PathName FWA_RIVER_NETWORK = PathName.newPathName("/FWA/FWA_RIVER_NETWORK");

  static String FWA_WATERSHED_CODE = "FWA_WATERSHED_CODE";

  static String GEOMETRY = "GEOMETRY";

  static GeometryFactory GEOMETRY_FACTORY = GeometryFactory.fixed2d(3005, 0, 0);

  static String GNIS_ID = "GNIS_ID";

  static String GNIS_NAME = "GNIS_NAME";

  static String LENGTH_METRE = "LENGTH_METRE";

  static String LINEAR_FEATURE_ID = "LINEAR_FEATURE_ID";

  static String LOCAL_WATERSHED_CODE = "LOCAL_WATERSHED_CODE";

  static String MAX_LOCAL_WATERSHED_CODE = "MAX_LOCAL_WATERSHED_CODE";

  static String MIN_LOCAL_WATERSHED_CODE = "MIN_LOCAL_WATERSHED_CODE";

  static String ROUTES = "ROUTES";

  static String STREAM_ORDER = "STREAM_ORDER";

  static String UPSTREAM_LENGTH = "UPSTREAM_LENGTH";

  static String UPSTREAM_ROUTE_MEASURE = "UPSTREAM_ROUTE_MEASURE";

  static String WATERSHED_CODE = "WATERSHED_CODE";
}
