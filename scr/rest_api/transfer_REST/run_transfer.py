from pymongo import MongoClient

codeString = '''DOMAIN = {
    '''

client = MongoClient("localhost", 27017, maxPoolSize=50)

cols = client.cota_transfer.collection_names()
for todayDate in cols:
    codeString = codeString + "'" + todayDate + "': {'datasource': {" + "'source': '" + todayDate + "'}},"+'''
    '''

codeString=codeString +"}"





codeString = codeString + '''
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DBNAME = 'cota_transfer'

ALLOW_UNKNOWN=True

PAGINATION_LIMIT = 10000

PAGINATION_DEFAULT = 10000'''


F = open("./setting.py","w")
F.write(codeString)

F.close()