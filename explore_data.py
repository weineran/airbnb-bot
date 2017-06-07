from optparse import OptionParser
from pprint import pprint
from datetime import date, timedelta

from pymongo import MongoClient, collection, cursor

from utilities import convert_yyyymmdd_to_date, convert_date_to_yyyy_mm_dd, convert_date_to_yyyymmdd, daterange

MONGO_URI = "mongodb://0.0.0.0:27017"


def parse_args():
    parser = OptionParser(usage="Usage: %prog [options]",
                          description="Try to print occupancy information.")
    parser.add_option("-s", "--start-date",
                      dest="start_date", default="20170601", type=str,
                      help="How far back in time to explore data from.  Provide a date in 'yyyymmdd' format.")
    parser.add_option("-e", "--end-date",
                      dest="end_date", default="20170604", type=str,
                      help="The end date of the data to be explored.  Provide a date in 'yyyymmdd' format.")

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

    dates = daterange(start_date, end_date)
    print("Dates: %s thru %s" % (start_date, end_date))

    for scrape_date in dates:
        print("scrape date: %s" % scrape_date)
        collection_name = convert_date_to_yyyymmdd(scrape_date)

        if collection_name not in database.collection_names():
            print("Collection [%s] not in database [%s].  Skipping." % (collection_name, database.name))
            continue

        this_collection = database.get_collection(collection_name)

        aggregate_data[collection_name] = {"total_listings": [], "available_listings": [], "occupancy": []}

        checkin_date_start = start_date
        checkin_date_end = start_date + timedelta(days=120)

        listing_counts = aggregate_data[collection_name]

        for checkin_date in daterange(checkin_date_start, checkin_date_end):
            checkout_date = checkin_date + timedelta(days=1)

            checkin_date_str = convert_date_to_yyyy_mm_dd(checkin_date)
            checkout_date_str = convert_date_to_yyyy_mm_dd(checkout_date)

            total_query_filter = {"pricing_quote.checkin": checkin_date_str, "pricing_quote.checkout": checkout_date_str}
            number_of_total_listings = this_collection.find(filter=total_query_filter).count()

            available_query_filter = {"pricing_quote.checkin": checkin_date_str,
                                      "pricing_quote.checkout": checkout_date_str,
                                      "pricing_quote.available": True}
            number_of_available_listings = this_collection.find(filter=available_query_filter).count()

            listing_counts["total_listings"].append(number_of_total_listings)
            listing_counts["available_listings"].append(number_of_available_listings)

            number_of_occupied_listings = number_of_total_listings - number_of_available_listings

            if number_of_total_listings == 0:
                occupancy_rate = 0
            else:
                occupancy_rate = float(number_of_occupied_listings)/float(number_of_total_listings)

            listing_counts["occupancy"].append(occupancy_rate)

        #print(listing_counts)

    print(aggregate_data)
