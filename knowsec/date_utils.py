from datetime import datetime as dt
from datetime import timedelta
from pandas.tseries.offsets import BDay as bus_day
import pytz


def today_utc(add_tz=True):
    result = dt.utcnow()

    if add_tz is True:
        result = pytz.utc.localize(result)

    return result.date()


def today_dt_utc(add_tz=True):
    result = dt.utcnow()

    if add_tz is True:
        result = pytz.utc.localize(result)

    return result

def now_utc(add_tz=True):
    result = dt.utcnow()

    if add_tz is True:
        result = pytz.utc.localize(result)

    return result


def last_business_day(local=True, n=1, add_tz=True):
    return (today_utc(local, add_tz) - bus_day(n=n)).to_pydatetime().date()


def shift(date_time, n=1, unit='day', shift_backwards=True):
    if unit == 'year':
        dt_obj = timedelta(days=365 * n)

    elif unit == 'day':
        dt_obj = timedelta(days=n)

    elif unit == 'busday':
        dt_obj = bus_day(n=n)

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

    return result
