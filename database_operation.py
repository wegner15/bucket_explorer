"""
This module handles all database operations for the application
File metadata will be stored on the database to facilitate advance analysis
TODO: Add error handling
TODO: Add support for other databases
"""

from datetime import datetime

from pymongo import MongoClient
uri = "mongodb://localhost:27017/" # Your mongo database url
# TODO: Integrate .env for the mongo url for production
client = MongoClient(uri)


def database_exists(db_name):
    """
    Check if a database exists
    :param db_name:
    :return: True if exists False if not
    """
    return db_name in client.list_database_names()
def create_database(db_name):
    """
    Create a new database
    :param db_name:
    :return: Mongo database instance
    """
    db = client[db_name]
    return db
def create_collection(db_name, collection_name):
    """
    Create a new collection
    :param db_name:
    :param collection_name:
    :return: Collection instance
    """
    db = client[db_name]
    collection = db[collection_name]
    return collection
def insert_many(db_name, collection_name, data):
    """
    Insert many documents into a collection
    TODO: Add support for other databases
    :param db_name:
    :param collection_name:
    :param data:
    :return:
    """
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)

def insert_one(db_name, collection_name, data):
    """
    Insert one document into a collection
    :param db_name:
    :param collection_name:
    :param data:
    :return:
    """
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_one(data)

def get_last_item(db_name, collection_name):
    """
    Get the last item in a collection
    :param db_name:
    :param collection_name:
    :return:
    """
    if not database_exists(db_name):
        return None
    db = client[db_name]
    collection = db[collection_name]
    # Collect the last item
    last_item = collection.find().sort('_id', -1).limit(1)
    return last_item
def get_document_count(db_name, collection_name):
    """
    Get the number of documents in a collection
    :param db_name:
    :param collection_name:
    :return: Integer
    """
    db = client[db_name]
    collection = db[collection_name]
    count = collection.count_documents({})
    return count

def get_items(db_name, collection_name, limit=1000, offset=0):
    """
    Get the last item in a collection
    :param db_name:
    :param collection_name:
    :param limit:
    :param offset:
    :return: Collection item
    """
    db_obj = client[db_name]
    collection_obj = db_obj[collection_name]
    # Collect the last item
    last_item = collection_obj.find().sort('_id', -1).limit(limit).skip(offset)
    return last_item

def update_by_id(db_name, collection_name, _id, data):
    """
    Update a document by id
    :param db_name:
    :param collection_name:
    :param _id:
    :param data:
    :return:
    """
    db = client[db_name]
    collection = db[collection_name]
    collection.update_one({"_id": _id}, {"$set": data})

def get_items_from_date(db_name, collection_name, date, limit=1000,
                        date_field="LastModified"):
    """
    Get the last item in a collection
    :param db_name:
    :param collection_name:
    :param date:
    :param limit:
    :param date_field:
    :return:
    """
    db = client[db_name]
    collection = db[collection_name]
    # Collect the last item
    last_item = collection.find({date_field: {"$gte": date}}).sort('_id', -1).limit(limit)
    return last_item


if __name__ == '__main__':
    """
    Test the database operations, including various queries
    """
    database="littleimages"
    files_collection="fileList"
    metadata_collection="metadata"

    # count=get_document_count(database, files_collection)
    # print(count)

    filter_date= datetime(2024, 1, 1)
    items=get_items_from_date(database, files_collection, date=filter_date,
                              limit=100, date_field="Last-Modified")
    no_of_items = 0
    unique_extensions=[]
    collected_special_files=[]
    for item in items:
        no_of_items+=1
        if item.get('Content-Type') not in unique_extensions:
            unique_extensions.append(item.get('Content-Type'))
            collected_special_files.append(item.get('Url'))

        print(item.get('Url'))

    print("no of items:",no_of_items)
    print("unique extensions",unique_extensions)
    print("collected special files",collected_special_files)
