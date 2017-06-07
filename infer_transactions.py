from optparse import OptionParser
from pprint import pprint
from datetime import date, timedelta

from pymongo import MongoClient, collection, cursor

from utilities import convert_yyyymmdd_to_date, convert_date_to_yyyy_mm_dd, convert_date_to_yyyymmdd, daterange

MONGO_URI = "mongodb://0.0.0.0:27017"


def parse_args():
    parser = OptionParser(usage="Usage: %prog [options]",
                          description="Infer airbnb transactions and save data to MongoDB")
    parser.add_option("-s", "--start-date",
                      dest="start_date", default="20170601", type=str,
                      help="The date to start looking for transactions.  Provide a date in 'yyyymmdd' format.")
    parser.add_option("-e", "--end-date",
                      dest="end_date", default="20170604", type=str,
                      help="The date to stop looking for transactions.  Provide a date in 'yyyymmdd' format.")

    return parser.parse_args()


if __name__ == "__main__":
    (options, args) = parse_args()
    start_date_str = options.start_date
    end_date_str = options.end_date

    assert len(start_date_str) == 8, "Invalid start_date [%s]. Be sure to use 'yyyymmdd' format." % start_date_str
    assert len(end_date_str) == 8, "Invalid end_date [%s]. Be sure to use 'yyyymmdd' format." % end_date_str
    start_date = convert_yyyymmdd_to_date(start_date_str)
    end_date = convert_yyyymmdd_to_date(end_date_str)

    mongo_client = MongoClient(MONGO_URI)

    database = mongo_client["air_bnb"]

    aggregate_data = {}

    checkin_date = start_date + timedelta(days=50)
    checkout_date = start_date + timedelta(days=51)

    checkin_date_str = convert_date_to_yyyy_mm_dd(checkin_date)
    checkout_date_str = convert_date_to_yyyy_mm_dd(checkout_date)

    dates = daterange(start_date, end_date)
    print("Dates: %s thru %s" % (start_date, end_date))

    for scrape_date in dates:
        if scrape_date >= checkin_date:
            break

        print("scrape date: %s" % scrape_date)
        collection_name = convert_date_to_yyyymmdd(scrape_date)

        if collection_name not in database.collection_names():
            print("Collection [%s] not in database [%s].  Skipping." % (collection_name, database.name))
            continue

        this_collection = database.get_collection(collection_name)

        available_query_filter = {"pricing_quote.checkin": checkin_date_str,
                                  "pricing_quote.checkout": checkout_date_str,
                                  "pricing_quote.available": True}

        available_listings = this_collection.find(filter=available_query_filter)  # get all documents

        for document in available_listings:
            listing_id = document["listing"]["id"]

            if listing_id not in aggregate_data:
                aggregate_data[listing_id] = []

            total_price = document["pricing_quote"]["total_price"]

            aggregate_data[listing_id].append(total_price)

    print(aggregate_data)
