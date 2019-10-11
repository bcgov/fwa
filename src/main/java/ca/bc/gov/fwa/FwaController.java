package ca.bc.gov.fwa;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.schema.RecordStore;

public class FwaController {

  public static final String FWA_ROUTES_SP = "/WHSE_BASEMAPPING/FWA_ROUTES_SP";

  public static final String FWA_WATERSHED_CODE = "FWA_WATERSHED_CODE";

  public static RecordStore getBcgwRecordStore() {
    return getRecordStore("jdbc:postgresql://localhost/bcgw", "pxaustin", "Tdnmatm1");
  }

  public static RecordStore getFileRecordStore(final String file) {
    final RecordStore recordStore = RecordStore.newRecordStore(file);
    recordStore.initialize();
    return recordStore;
  }

  public static RecordStore getFwaFgdbRecordStore() {
    return getFileRecordStore("/opt/data/FWA/FWA_BC.gdb");
  }

  public static RecordStore getFwaRecordStore() {
    return getRecordStore("jdbc:postgresql://localhost/fwa", "pxaustin", "Tdnmatm1");
  }

  public static RecordStore getRecordStore(final String url, final String user,
    final String password) {
    final MapEx properties = new LinkedHashMapEx() //
      .add("url", url)//
      .add("user", user)//
      .add("password", password)//
    ;
    final RecordStore recordStore = RecordStore.newRecordStore(properties);
    recordStore.initialize();
    return recordStore;
  }

}
