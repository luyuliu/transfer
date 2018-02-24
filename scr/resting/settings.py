DOMAIN = {
    'agency': {},
    'calendar': {},
    'calendar_dates': {},
    'fare_attributes': {},
    'fare_rules': {},
    'routes': {},
    'shapes': {},
    'stop_times': {},
    'stops': {},
    'transfer': {'datasource':{
        "source":'transfer'
    }
    },
    'trips': {},
    'tripupdate': {},
    'vehicle': {}
}

# Let's just use the local mongod instance. Edit as needed.

# Please note that MONGO_HOST and MONGO_PORT could very well be left
# out as they already default to a bare bones local 'mongod' instance.
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

# Skip these if your db has no auth. But it really should.
#MONGO_USERNAME = '<your username>'
#MONGO_PASSWORD = '<your password>'

MONGO_DBNAME = 'cota_gtfs_1'

ALLOW_UNKNOWN=True