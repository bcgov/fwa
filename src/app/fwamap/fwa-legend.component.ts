import {
  AfterViewInit,
  Component,
  ElementRef,
  ViewChild
} from '@angular/core';

import * as L from 'leaflet';
import {MapService} from 'revolsys-angular-leaflet';

const Legend = L.Control.extend({
  options: {
    position: 'topleft'
  },

  onRemove: function(map) {
    this._container = null;
  },

  onAdd: function(map) {

    this._map = map;
    const container = this._container = L.DomUtil.create('div', 'leaflet-bar leaflet-control');
    this._initToggle();

    return container;
  },

  _initToggle: function() {

    const container = this._container;

    container.setAttribute('aria-haspopup', true);

    if (!L.Browser.touch) {
      L.DomEvent
        .disableClickPropagation(container);
    } else {
      L.DomEvent.on(container, 'click', L.DomEvent.stopPropagation);
    }

    if (!L.Browser.android) {
      L.DomEvent
        .on(container, 'mouseover', this._expand, this)
        .on(container, 'mouseout', this._collapse, this);
    }
    const link = L.DomUtil.create('a', 'legend-toggle', container);
    link.innerHTML = '<span class="fa fa-info"></span>';
    link.href = '#';
    link.title = 'Legend';
    link.setAttribute('role', 'button');
    link.setAttribute('aria-label', link.title);
    L.DomEvent.disableClickPropagation(link);
    this._legend = container.appendChild(this.options['legendElement']);
    if (L.Browser.touch) {
      L.DomEvent
        .on(link, 'click', L.DomEvent.stop)
        .on(link, 'click', this._expand, this);
    } else {
      L.DomEvent.on(link, 'focus', this._expand, this);
    }

    this._map.on('click', this._collapse, this);
  },

  _expand: function() {
    this._legend.style.display = 'block';
    this._legend.style.position = 'absolute';
  },

  _collapse: function() {
    this._legend.style.display = 'none';
  },
});

@Component({
  selector: 'app-fwa-legend',
  template: `<div #legendElement style="display:none;line-height:0px;margin-left:-3px">
  <svg width="300" height="265" style="background-color:white;border-radius: 4px;border:2px solid rgba(0,0,0,0.2)">
    <ng-container *ngFor="let item of legend; let i = index">
      <line *ngIf="item[1]==='line'" x1="10" [attr.y1]="(i+1)*20" x2="40" [attr.y2]="(i+1)*20" [ngClass]="item[2]" />
      <circle *ngIf="item[1]==='circle'" cx="25" [attr.cy]="(i+1)*20" r="6" [ngClass]="item[2]" />
      <text x="40" [attr.y]="(i+1)*20" transform="translate(8,4)">{{item[0]}}</text>
    </ng-container>
  </svg>
</div>`,
  styleUrls: ['./fwa-legend.component.css']
})
export class FwaLegendComponent implements AfterViewInit {
  @ViewChild('legendElement') legendElement: ElementRef;

  legend = [
    ['Stream', 'line', 'legend-stream'],
    ['Higlighted Stream', 'line', 'legend-higlighted-stream'],
    ['Higlighted Stream (Upstream)', 'line', 'legend-higlighted-stream-upstream'],
    ['Higlighted Stream (Downstream)', 'line', 'legend-higlighted-stream-downstream'],
    ['Selected Stream', 'line', 'legend-selected-stream'],
    ['Selected Stream (Upstream)', 'line', 'legend-selected-stream-upstream'],
    ['Selected Stream (Downstream)', 'line', 'legend-selected-stream-downstream'],
  ];

  legendControl: any;

  constructor(
    private mapService: MapService
  ) {
  }

  ngAfterViewInit() {
    this.legendControl = new Legend({
      legendElement: this.legendElement.nativeElement
    });
    this.mapService.withMap(map => {
      map.addControl(this.legendControl);
    });
  }
}

