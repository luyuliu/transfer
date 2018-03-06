DOMAIN = {
    'agency': {'datasource': {
        "source": 'agency'}},
    'calendar': {'datasource': {
        "source": 'calendar'}},
    'calendar_dates': {'datasource': {
        "source": 'calendar_dates'}},
    'fare_attributes': {'datasource': {
        "source": 'fare_attributes'}},
    'fare_rules': {'datasource': {
        "source": 'fare_rules'}},
    'routes': {'datasource': {
        "source": 'routes'}},
    'shapes': {'datasource': {
        "source": 'shapes'}},
    'stop_times': {'datasource': {
        "source": 'stop_times'}},
    'stops': {'datasource': {
        "source": 'stops'} },
    'transfer': {'datasource': {
        "source": 'transfer'
    }
    },
    'trips': {'datasource': {
        "source": 'trips'}},
    'tripupdate': {'datasource': {
        "source": 'tripupdate'}},
    'vehicle': {'datasource': {
        "source": 'vehicle'}}
}

# Let's just use the local mongod instance. Edit as needed.

# Please note that MONGO_HOST and MONGO_PORT could very well be left
# out as they already default to a bare bones local 'mongod' instance.
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

# Skip these if your db has no auth. But it really should.
# MONGO_USERNAME = '<your username>'
# MONGO_PASSWORD = '<your password>'

MONGO_DBNAME = 'cota_gtfs_1'

ALLOW_UNKNOWN = True

PAGINATION_LIMIT = 10000

PAGINATION_DEFAULT = 10000
