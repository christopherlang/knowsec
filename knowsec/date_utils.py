import datetime
from datetime import timedelta
import pandas
import pytz
import re

_BUSDAYOFFSET = pandas.tseries.offsets.BDay


def timezones(regex_q='^US|(^UTC$)', ignore_case=False):
    regex_flag = re.I if ignore_case else 0
    return [i for i in pytz.all_timezones if re.search(regex_q, i, regex_flag)]


def today_utc():
    result = datetime.datetime.utcnow()

    return result.date()


def today(timezone='US/Eastern'):
    return now(timezone=timezone).date()


def now_utc():
    return pytz.utc.localize(datetime.datetime.utcnow())


def now(timezone='US/Eastern'):
    to_tz = pytz.timezone(timezone)
    return now_utc().astimezone(to_tz)


def last_busday_utc():
    return (today_utc() - _BUSDAYOFFSET(n=1)).to_pydatetime().date()


def last_busday(timezone='US/Eastern'):
    result = (today(timezone) - _BUSDAYOFFSET(n=1)).to_pydatetime()
    return result.date()


def is_business_day(date):
    return bool(len(pandas.bdate_range(date, date)))


def lead(date_time, n=1, unit='day'):
    return shift(date_time, n, unit, False)


def lag(date_time, n=1, unit='day'):
    return shift(date_time, n, unit, True)


def shift(date_time, n=1, unit='day', shift_backwards=True):
    if unit == 'year':
        dt_obj = timedelta(days=365 * n)

    elif unit == 'day':
        dt_obj = timedelta(days=n)

    elif unit == 'busday':
        dt_obj = _BUSDAYOFFSET(n=n)

    elif unit == ' week':
        dt_obj = timedelta(weeks=n)

    elif unit == 'month':
        dt_obj = timedelta(days=30 * n)

    elif unit == 'quarter':
        dt_obj = timedelta(days=90 * n)

    elif unit == 'minute':
        dt_obj = timedelta(minutes=n)

    elif unit == 'second':
        dt_obj = timedelta(seconds=n)

    elif unit == 'hour':
        dt_obj = timedelta(hours=n)

    else:
        raise ValueError('incorrect unit value')

    if unit != 'busday':
        if shift_backwards is True:
            result = date_time - dt_obj
        else:
            result = date_time + dt_obj

    elif unit == 'busday':
        if shift_backwards is True:
            result = (date_time - dt_obj).to_pydatetime()
        else:
            result = (date_time + dt_obj).to_pydatetime()

        if type(date_time) is datetime.date:
            result = result.date()

    return result
