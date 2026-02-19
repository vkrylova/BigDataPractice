from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# All entries where the “content” field is less than 5 characters long;

client = MongoClient('mongodb://user:pass@localhost:27017/')
result = client['airflow']['reviews'].aggregate([
    {
        '$match': {
            '$expr': {
                '$lt': [
                    {
                        '$strLenCP': '$content'
                    }, 5
                ]
            }
        }
    }
])
