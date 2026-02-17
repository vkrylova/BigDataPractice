from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# Top 5 frequently occurring comments

client = MongoClient('mongodb://user:pass@localhost:27017/')
result = client['airflow']['reviews'].aggregate([
    {
        '$group': {
            '_id': '$content',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 5
    }
])