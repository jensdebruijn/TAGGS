import pytz
import datetime
import operator


def isoformat_2_date(datestr):
    return datetime.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')


def daterange(start_date, end_date, timedelta, ranges=False, include_last=False, UTC=False):
    if UTC:
        start_date = start_date.replace(tzinfo=pytz.UTC)
        end_date = end_date.replace(tzinfo=pytz.UTC)
    if not isinstance(timedelta, datetime.timedelta):
        timedelta = datetime.timedelta(seconds=int(timedelta))
    if include_last:
        sign = operator.le
    else:
        sign = operator.lt
    while sign(start_date, end_date):
        if ranges:
            yield start_date, start_date + timedelta
        else:
            yield start_date
        start_date += timedelta


def date_handler(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")
