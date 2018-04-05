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
  center: [39.98, -83],
  layers: [baseLayer],
  zoomControl: false,
  attributionControl: false,
  maxZoom: 18
});

var arrow = L.polyline([[57, -19], [60, -12]], {}).addTo(map);
/*
var arrowHead = L.polylineDecorator(arrow, {
  patterns: [
    { offset: '100%', repeat: 0, symbol: L.Symbol.arrowHead({ pixelSize: 15, polygon: false, pathOptions: { stroke: true } }) }
  ]
}).addTo(map);*/


stopsLayer = new L.markerClusterGroup({
  spiderfyOnMaxZoom: true,
  showCoverageOnHover: false,
  zoomToBoundsOnClick: true,
  disableClusteringAtZoom: 15,
});




$(document).ready(function () {
  $('#date-input').val(("2018-01-29"))
})






function createCORSRequest(method, url) {
  var xhr = new XMLHttpRequest();
  if ("withCredentials" in xhr) {
    xhr.open(method, url, true);

  } else if (typeof XDomainRequest != "undefined") {
    xhr = new XDomainRequest();
    xhr.open(method, url);

  } else {
    xhr = null;

  }
  return xhr;
}

function returnPolygonLayer(transfer,e) {
  var b_stop_id=transfer.b_stop_id
  var b_lat=e.latlng.lat
  var b_lng=e.latlng.lng
  
  var arrow = L.polyline([[, ], [b_lat, b_lng]], {}).addTo(map);



}


$("#start-btn").click(function () {
  /*
 console.log("create")
 xhr = createCORSRequest('GET', "http://127.0.0.1:5001/");
 if (!xhr) {
   throw new Error('CORS not supported');
 }

 xhr.onload = function() {
   var text = xhr.responseText;
   var title = getTitle(text);
   alert('Response from CORS request to ' + url + ': ' + title);
 };*/

  todayDate = $("#date-input").val()
  console.log(todayDate)

  $.get("http://127.0.0.1:5001/stops", function (rawstops) {
    stops = rawstops._items


    data1 = {}
    data1.type = "FeatureCollection"
    data1.features = []
    for (var i = 0; i < stops.length; i++) {
      everyStop = {}
      everyStop.type = "Feature"
      everyStop.geometry = {}
      everyStop.geometry.type = "Point"
      everyStop.geometry.coordinates = [parseFloat(stops[i].stop_lon), parseFloat(stops[i].stop_lat)]
      everyStop.properties = {}
      everyStop.properties.stop_name = stops[i].stop_name
      everyStop.properties.stop_id = stops[i].stop_id
      everyStop.properties.stop_id = stops[i].stop_id
      data1.features.push(everyStop)
    }

    //data1=JSON.stringify(data1)
    console.log(data1)

    stopsFullLayer = L.geoJson(data1, {
      pointToLayer: function (feature, latlng) {
        return L.marker(latlng, {
          icon: L.icon({
            iconUrl: "./img/bus.png",
            iconSize: [14, 14],
            iconAnchor: [7, 14],
            popupAnchor: [0, -25]
          }),
          riseOnHover: true,
        });
      },
      onEachFeature: function (feature, layer) {
        if (feature.properties) {
          layer.on({
            click: function (e) {
              var transferURL = 'http://127.0.0.1:5002/trips?where={"b_stop_id":"' + feature.properties.stop_id + '"}'

              $.get(transferURL, function (rawtransfers) {
                console.log("xixi")
                transfers = rawtransfers._items
                console.log(transfers)
                for (var i = 0; i < transfers.length; i++) {
                  eval(feature.properties.stop_id + i + "Layer=returnPolygonLayer(transfers[i],e)")
                }

              })
            }
          });

        }
      }
    });
    stopsLayer.addLayer(stopsFullLayer)
    map.addLayer(stopsLayer);
  });
});


//read in the stop transfer pair
