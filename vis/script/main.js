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

function switchStatus(status,line) {
  switch (status) {
    case 0:
      line.zero_count++;

      break;

    case 1:
      line.one_count++;

      break;

    case 2:
      line.two_count++;

      break;

    default:
      line.miss_count++;
  }
  line.total_count++;
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

function returnPolygonLayer(transfer, e) {
  var b_stop_id = transfer.b_stop_id
  var b_lat = e.latlng.lat
  var b_lng = e.latlng.lng

  var arrow = L.polyline([[,], [b_lat, b_lng]], {}).addTo(map);



}

var tran;

$("#start-btn").click(function () {
//$(document).ready(function () {
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

  todayDate = $("#date-input").val().replace('-', '').replace('-', '')
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
    //console.log(data1)

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
        //if (feature.properties) {
        layer.on({
          click: function (e) {
            var transferURL = 'http://127.0.0.1:5003/' + todayDate + '/?where={"a_stop_id":"' + feature.properties.stop_id + '"}';
            $.get(transferURL, function (rawTransfers) {
              transfers = rawTransfers._items;
              tran = transfers//debug
              console.log(transfers.length);
              stopsDic = {};// The dictionary of stops pair for a stop.
              for (var i in transfers) {
                console.log(transfers[i].b_stop_id);
                if (stopsDic[transfers[i].b_stop_id] === undefined) {
                  // The start of initialization.
                  var line = {}
                  var stopDetail = stops.filter(function (item) { return (item.stop_id == transfers[i].b_stop_id); })[0];
                  //console.log(stops)
                  line.lat = stopDetail.stop_lat;
                  line.lon = stopDetail.stop_lon;
                  line.total_count = 0;
                  line.zero_count = 0;
                  line.one_count = 0;
                  line.two_count = 0;
                  line.miss_count = 0;
                  // The end of initialization

                  switchStatus(transfers[i].status, line);

                  stopsDic[transfers[i].b_stop_id] = line
                }
                else {
                  switchStatus(transfers[i].status, stopsDic[transfers[i].b_stop_id]);
                }
              }

              console.log(stopsDic)

              var contentString="";
              for (var j=0;j<Object.keys(stopsDic).length;j++){
                console.log(Object.keys(stopsDic)[j])
                contentString=contentString+"<h3>"+Object.keys(stopsDic)[j]+"</h3>"
                contentString=contentString+"Total: "+stopsDic[Object.keys(stopsDic)[j]].total_count
                +"</br>Miss: "+stopsDic[Object.keys(stopsDic)[j]].miss_count
                +"</br>Timeless: "+stopsDic[Object.keys(stopsDic)[j]].zero_count
                +"</br>ATP: "+stopsDic[Object.keys(stopsDic)[j]].one_count
                +"</br>False_timeless: "+stopsDic[Object.keys(stopsDic)[j]].two_count

              }
              
              var popup = L.popup().setLatLng([feature.geometry.coordinates[1], feature.geometry.coordinates[0]]).setContent(contentString).openOn(map);
              //var popup = new L.Popup().setLatlng(e.latlng).setContent(contentString);
              popup.openOn(map);
            })

          }
        });

        //}
      }
    });
    stopsLayer.addLayer(stopsFullLayer)
    map.addLayer(stopsLayer);
  });
});


//read in the stop transfer pair
