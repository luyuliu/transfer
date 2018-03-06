$("#sidebar-hide-btn").click(function () {
  animateSidebar();
  $('.mini-submenu').fadeIn();
  return false;
});


$('.mini-submenu').on('click', function () {
  animateSidebar();
  $('.mini-submenu').hide();
})

function animateSidebar() {
  $("#sidebar").animate({
    width: "toggle"
  }, 350, function () {
    map.invalidateSize();
  });
}



var baseLayer = L.esri.basemapLayer('Topographic')
map = L.map("map", {
  zoom: 13,
  center: [40.68510, -73.94136],
  layers: [baseLayer],
  zoomControl: false,
  attributionControl: false,
  maxZoom: 18
});


/*
stopsLayer = new L.markerClusterGroup({
  spiderfyOnMaxZoom: true,
  showCoverageOnHover: false,
  zoomToBoundsOnClick: true,
  disableClusteringAtZoom: 16,
  clusterPane: layerID + "Pane"
});
stopsFullLayer = L.geoJson(null, {
  pointToLayer: function (feature, latlng) {
    return L.marker(latlng, {
      icon: L.icon({
        iconUrl: "./img/bus.png",
        iconSize: [14, 14],
        iconAnchor: [7, 14],
        popupAnchor: [0, -25]
      }),
      title: feature.properties.name,
      riseOnHover: true,
      pane: layerID + "Pane"
    });
  },
  onEachFeature: function (feature, layer) {
    if (feature.properties) {
      var content = "<h4>" + "Station Name: " + feature.properties.name + "<br/>" + "Available Bikes: " + feature.properties.availableBikes + "<br/>" + "Available Docks: " + feature.properties.availableDocks + "<br/>" + "Last Checked: " + feature.properties.timestamp + "</h4><br/>" +
        "<!--Streetview Div-->" +
        "<div  id='streetview' style='margin-top:10px;'><img class='center-block' src='https://maps.googleapis.com/maps/api/streetview?size=300x300&location=" + layer.getLatLng().lat + "," + layer.getLatLng().lng + "&key=AIzaSyCewGkupcv7Z74vNIVf05APjGOvX4_ygbc' height='300' width='300'></img><hr><h4 class='text-center'><a href='http://maps.google.com/maps?q=&layer=c&cbll=" + layer.getLatLng().lat + "," + layer.getLatLng().lng + "' target='_blank'>Google Streetview</a></h4</div>";


      layer.on({
        click: function (e) {
          var popup = L.popup().setLatLng([feature.geometry.coordinates[1], feature.geometry.coordinates[0]]).setContent(content).openOn(map);
        }
      });
      $("#feature-list tbody").append('<tr class="feature-row" layerID="' + layerID + '" id="' + L.stamp(layer) + '" lat="' + layer.getLatLng().lat + '" lng="' + layer.getLatLng().lng + '"><td style="vertical-align: middle;"><img width="18" height="18" src="img/stops.png"></td><td class="feature-name">' + layer.feature.properties.name + '</td><td style="vertical-align: middle;"><i class="fa fa-chevron-right pull-right"></i></td></tr>');
      theaterSearch.push({
        name: layer.feature.properties.NAME,
        address: layer.feature.properties.ADDRESS1,
        source: "Theaters",
        id: L.stamp(layer),
        lat: layer.feature.geometry.coordinates[1],
        lng: layer.feature.geometry.coordinates[0]
      });
    }
  }
});
$.get("https://luyuliu.github.io/CURIO-Map/data/COGOArrayGeoJSON.json", function (data) {
  stopsFullLayer.addData(data);
  stopsLayer.addLayer(stopsFullLayer)
  map.addLayer(stopsLayer);
});*/

//read in the stop transfer pair
