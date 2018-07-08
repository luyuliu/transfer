DOMAIN = {
    'transfer': {'datasource': {'source': 'transfer'}},
    'routes': {'datasource': {'source': 'routes'}},
    'calendar_dates': {'datasource': {'source': 'calendar_dates'}},
    'fare_attributes': {'datasource': {'source': 'fare_attributes'}},
    'shapes': {'datasource': {'source': 'shapes'}},
    'trips': {'datasource': {'source': 'trips'}},
    'stop_times': {'datasource': {'source': 'stop_times'}},
    'stops': {'datasource': {'source': 'stops'}},
    'fare_rules': {'datasource': {'source': 'fare_rules'}},
    'agency': {'datasource': {'source': 'agency'}},
    'calendar': {'datasource': {'source': 'calendar'}},
    }
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DBNAME = 'cota_gtfs_1'

ALLOW_UNKNOWN=True

PAGINATION_LIMIT = 10000

PAGINATION_DEFAULT = 10000