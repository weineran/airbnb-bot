from datetime import date, timedelta


def convert_yyyymmdd_to_date(yyyymmdd):
    assert isinstance(yyyymmdd, str)

    return date(int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))


def convert_date_to_yyyymmdd(a_date):
    assert isinstance(a_date, date)

    return str(a_date.year).zfill(4) + str(a_date.month).zfill(2) + str(a_date.day).zfill(2)


def convert_date_to_yyyy_mm_dd(a_date):
    assert isinstance(a_date, date)

    return str(a_date.year).zfill(4) + "-" + str(a_date.month).zfill(2) + "-" + str(a_date.day).zfill(2)


def daterange(start_date, end_date):
    assert isinstance(start_date, date) and isinstance(end_date, date)

    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)
