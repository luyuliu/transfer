var moment = require('../node_modules/moment')
var request = require('../node_modules/request');
var GtfsRealtimeBindings = require('../node_modules/gtfs-realtime-bindings');
function entityItem2geoJSON(item) {
  var locationPoint = {};
  locationPoint['type'] = 'Feature';
  locationPoint['geometry'] = {
    'type': 'Point',
    'coordinates': [+item.vehicle.position.longitude, +item.vehicle.position.latitude]
  };
  locationPoint['properties'] = {
    'trip_id': item.vehicle.trip.trip_id,
    'route_id': item.vehicle.trip.route_id,
    'vehicle_id': item.vehicle.vehicle.label,
    'bearing': item.vehicle.position.bearing,
    'speed': item.vehicle.position.speed,
    'current_status': item.vehicle.current_status,
    'timestamp': +item.vehicle.timestamp.low,
    'datetime': moment.unix(+item.vehicle.timestamp.low).format('YYYY-MM-DD HH:mm:ss'),
    'congestion_level': item.vehicle.congestion_level,
    'occupancy_status': item.vehicle.occupancy_status
  };

  if (locationPoint.properties.speed !== undefined || locationPoint.properties.speed !== null || locationPoint.properties.speed !== 'null') {
    locationPoint.properties.speed = Math.ceil(locationPoint.properties.speed * 3600 * 1000);
  }

  return locationPoint;
}

function featureCollection(feed) {

  var parsed = feed;

  var features = {};
  features['type'] = 'FeatureCollection';
  features['timegroup'] = null //to aggregate by timegroup
  features['features'] = [];
  parsed.entity.forEach(function (item) {
    features['features'].push(entityItem2geoJSON(item));
  });

  return features;
}


var requestSettings = [{
    method: 'GET',
    url: 'http://realtime.cota.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb',
    encoding: null
  },
  {
    method: 'GET',
    url: 'http://realtime.cota.com/TMGTFSRealTimeWebService/TripUpdate/TripUpdates.pb',
    encoding: null
  }
]
/*
function collectBusFeed() {
  request(requestSettings[0], function (error, response, body) {
    if (!error && response.statusCode == 200) {
      var feed = GtfsRealtimeBindings.FeedMessage.decode(body);
      var collection = featureCollection(feed)
      var now = Math.floor(new Date().getTime() / (1000 * 60)) * 60;
      collection.timegroup = now

      require('fs').writeFile('..\\data\\busfeed\\busfeed_' + now + '.geojson', JSON.stringify(collection), 'utf8', function (err) {
        if (err) {
          console.error(err);
        }
      });
    }
  });
}*/

function collectTripFeed() {
  request(requestSettings[1], function (error, response, body) {
    if (!error && response.statusCode == 200) {
      var collection = {};
      var now = Math.floor(new Date().getTime() / (1000 * 60)) * 60;
      collection.timegroup = now
      console.log(now)

      var feed = GtfsRealtimeBindings.FeedMessage.decode(body);
      collection.features = feed.entity;

      require('fs').writeFile('..\\..\\data\\tripfeed\\tripfeed_' + now + '.json', JSON.stringify(collection), 'utf8', function (err) {
        if (err) {
          console.error(err);
        }
      });
      return now;
    }
  });
}
/*
module.exports.collectBusFeed=collectBusFeed
module.exports.collectTripFeed=collectTripFeed*/
/*
var interval = setInterval(collectBusFeed, 60000);
setTimeout(function () {
  clearInterval(interval)
}, 8.64e+7);*/
//collectTripFeed()
var interval1 = setInterval(collectTripFeed, 60000);