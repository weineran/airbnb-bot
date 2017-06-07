from base64 import standard_b64decode
from pprint import pprint
from optparse import OptionParser
from datetime import date, timedelta
import sys
import os

from pymongo import MongoClient, collection, cursor
from pymongo.errors import BulkWriteError

from bson import objectid
from bson.son import SON

from utilities import convert_yyyymmdd_to_date, convert_date_to_yyyymmdd, daterange

SOURCE_URI = ""
DESTINATION_URI = "mongodb://0.0.0.0:27017"


def parse_args():
    parser = OptionParser(usage="Usage: %prog [options]",
                          description="Copy data from Trevor's mongodb to your locally running mongodb.")
    parser.add_option("-s", "--start-date",
                      dest="start_date", default="20170601", type=str,
                      help="How far back in time to copy data from.  Provide a date in 'yyyymmdd' format.")
    parser.add_option("-e", "--end-date",
                      dest="end_date", default="20170604", type=str,
                      help="The end date to copy data up to.  Provide a date in 'yyyymmdd' format.")

    return parser.parse_args()


def mongo_do_bulk_insert(target_collection, documents_to_insert):
    assert isinstance(target_collection, collection.Collection)
    assert isinstance(documents_to_insert, cursor.Cursor)

    print("Doing bulk insert of [%s] documents into destination [%s]" % (
        documents_to_insert.count(), target_collection.database.name + "." + target_collection.name))

    try:
        result = target_collection.insert_many(documents_to_insert)
    except BulkWriteError as bwe:
        pprint(bwe.details)
        exit()

    inserted_count = len(result.inserted_ids)

    if inserted_count == documents_to_insert.count():
        print("Successfully inserted all [%d] documents." % inserted_count)
    elif inserted_count < documents_to_insert.count():
        print("Not all insertions succeeded.  Inserted [%d] out of [%d] documents." % (
            inserted_count, documents_to_insert.count()))
    else:
        print("ERROR: Inserted [%d] documents, which is more than documents_to_insert.count() [%d]." % (
            inserted_count, documents_to_insert.count()))
        exit()


def mongo_id_already_exists(object_id, target_collection):
    assert isinstance(object_id, objectid.ObjectId)
    assert isinstance(target_collection, collection.Collection)

    num_matching_documents = target_collection.find(filter={"_id": object_id}, limit=1).count()

    if num_matching_documents == 1:
        return True
    elif num_matching_documents == 0:
        return False
    else:
        print("ERROR: find() with limit of 1 returned [%d] documents." % num_matching_documents)
        exit()


def mongo_do_iterative_insert(target_collection, documents_to_insert):
    assert isinstance(target_collection, collection.Collection)
    assert isinstance(documents_to_insert, cursor.Cursor)

    print("Doing iterative insert of [%s] documents into destination [%s]" % (
        documents_to_insert.count(), target_collection.database.name + "." + target_collection.name))

    summary = {"already_exists_count": 0, "inserted_count": 0, "failed_count": 0, "failed_list": []}
    for document in documents_to_insert:

        # move to global function
        object_id = document["_id"]
        if mongo_id_already_exists(object_id, target_collection):
            summary["already_exists_count"] += 1
            continue

        try:
            target_collection.insert_one(document)
        except:  # catch *all* exceptions
            e = sys.exc_info()[0]
            print("ERROR: %s" % e)
            summary["failed_count"] += 1
            summary["failed_list"].append(object_id)
        else:
            summary["inserted_count"] += 1

    if summary["inserted_count"] == documents_to_insert.count():
        print("Successfully inserted all [%s] documents" % documents_to_insert.count())
    else:
        print("Not all insertions succeeded. Insertion summary: %s" % str(summary))


if __name__ == "__main__":
    (options, args) = parse_args()
    start_date_str = options.start_date
    end_date_str = options.end_date

    assert len(start_date_str) == 8, "Invalid start_date [%s]. Be sure to use 'yyyymmdd' format." % start_date_str
    assert len(end_date_str) == 8, "Invalid end_date [%s]. Be sure to use 'yyyymmdd' format." % end_date_str
    start_date = convert_yyyymmdd_to_date(start_date_str)
    end_date = convert_yyyymmdd_to_date(end_date_str)

    # connect to source and destination MongoDBs
    path_to_certificate = "./ca.pem"
    assert os.path.exists(
        path_to_certificate), "[%s] does not exist.  You must copy an ssl certificate to the working directory.  " \
                              "To request an ssl certificate, contact the repo owner."
    mongo_client_source = MongoClient(SOURCE_URI, ssl_ca_certs="./ca.pem")
    mongo_client_destination = MongoClient(DESTINATION_URI)

    # grab reference to the databases in the source and destination
    database_source = mongo_client_source["air_bnb"]
    database_destination = mongo_client_destination["air_bnb"]

    # get list of collection names
    collection_names_source = database_source.collection_names()

    # loop through every collection, copying from source to destination
    for this_date in daterange(start_date, end_date):
        collection_name = convert_date_to_yyyymmdd(this_date)

        if collection_name not in collection_names_source:
            print("[%s] not in source [%s].  Skipping." % (collection_name, database_source.name))
            continue

        collection_source = database_source[collection_name]

        documents = collection_source.find()  # get all documents in the collection

        # case 1: the collection needs to be created in the destination
        if collection_name not in database_destination.collection_names():
            database_destination.create_collection(collection_name)
            result = mongo_do_bulk_insert(database_destination[collection_name], documents)
            continue

        # case 2: the collection already exists in the destination
        assert collection_name in database_destination.collection_names()
        collection_destination = database_destination.get_collection(collection_name)

        # case 2a: the *full* collection already exists in the destination
        if collection_destination.count() == collection_source.count():
            # ASSUMPTION: If the sizes match, the documents are identical
            print(
                "[%s] has already been copied.  Skipping." %
                (collection_destination.database.name + "." + collection_destination.name))
            continue

        # case 2b: the collection only partially exists in the destination
        mongo_do_iterative_insert(collection_destination, documents)
