from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# Average rating for each day (the result should be in timestamp type).

client = MongoClient('mongodb://user:pass@localhost:27017/')
result = client['airflow']['reviews'].aggregate([
    {
        '$addFields': {
            'at_ts': {
                '$toDate': '$at'
            }
        }
    }, {
        '$group': {
            '_id': {
                '$dateTrunc': {
                    'date': '$at_ts',
                    'unit': 'day'
                }
            },
            'avgScore': {
                '$avg': '$score'
            }
        }
    }, {
        '$project': {
            '_id': 0,
            'date': '$_id',
            'avgScore': 1
        }
    }
])