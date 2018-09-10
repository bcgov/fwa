import {
  Component,
  OnInit,
  ViewChild
} from '@angular/core';

import {RiverService} from '../river.service';

import {MatTabGroup, MatTab} from "@angular/material/tabs";

@Component({
  selector: 'app-side-bar',
  templateUrl: './side-bar.component.html',
  styleUrls: ['./side-bar.component.css']
})
export class SideBarComponent implements OnInit {

  @ViewChild('tabs') tabs: MatTabGroup;

  @ViewChild('tabRiver') tabRiver: MatTab;

  constructor(
    public riverService: RiverService,
  ) {
    this.riverService.selectedRiverLocations.subscribe((selectedRiver) => {
      this.tabRiver.disabled = selectedRiver === null;
      var selectedIndex = 0;
      if (selectedRiver != null) {
        selectedIndex = 2;
      }
      this.tabs.selectedIndex = selectedIndex;
    });
  }

  get river(): any {
    return this.riverService.selectedRiverLocations.riverFeature;
  }

  ngOnInit() {
  }

  km(length: number): string {
    return (length / 1000).toFixed(1) + ' km';
  }
}
