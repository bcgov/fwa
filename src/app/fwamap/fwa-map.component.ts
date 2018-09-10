import {
  Component,
  ElementRef,
  AfterViewInit,
  ViewChild
} from '@angular/core';
import {HttpClient} from '@angular/common/http';

import {
  CircleMarker,
  GeoJSON,
  Map,
  Popup
} from 'leaflet';
import * as L from 'leaflet';
import {TiledMapLayer} from 'esri-leaflet';
import {MapService} from 'revolsys-angular-leaflet';
import {RiverService} from '../river.service';

@Component({
  selector: 'app-map',
  templateUrl: './fwa-map.component.html',
  styleUrls: ['./fwa-map.component.css']
})
export class FwaMapComponent implements AfterViewInit {
  bcArcGisRestUrl = 'https://maps.gov.bc.ca/arcgis/rest/services';

  bcWmsUrl = 'https://openmaps.gov.bc.ca/geo/pub';

  @ViewChild('legend') legendElement: ElementRef;

  legend = [
    ['Stream', 'line', 'legend-stream'],
    ['Selected Stream', 'line', 'legend-selected-stream'],
    ['Selected Stream (Upstream)', 'line', 'legend-selected-stream-upstream'],
    ['Selected Stream (Downstream)', 'line', 'legend-selected-stream-downstream'],
    ['Higlighted Stream', 'line', 'legend-higlighted-stream'],
    ['Higlighted Stream (Upstream)', 'line', 'legend-higlighted-stream-upstream'],
    ['Higlighted Stream (Downstream)', 'line', 'legend-higlighted-stream-downstream'],
  ];
  constructor(
    private http: HttpClient,
    private riverService: RiverService,
    private mapService: MapService
  ) {
  }

  ngAfterViewInit() {
    L.Icon.Default.imagePath = '/';
    L.Icon.Default.mergeOptions({
      iconRetinaUrl: 'assets/marker-icon-2x.png',
      iconUrl: 'assets/marker-icon.png',
      shadowUrl: 'assets/marker-shadow.png',
    });

    this.riverService.init();
  }

  private tempLayersInit() {
    this.mapService.withMap(map => {
      const location = [52.513437, -121.596309];
      const mtPolleyMarker = new CircleMarker(location, {
        title: 'Mt. Polley Mine',
        weight: 7
      }).addTo(map);
      mtPolleyMarker.bindPopup('Mt. Polley Mine').openPopup();
      map.setView(location, 10);
    });
  }
}

