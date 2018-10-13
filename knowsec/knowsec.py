import sys
import backoff
import yaml
import dfu
import database
import tqdm
import pandas
from pandas.tseries.frequencies import BDay as bus_day
import datetime as dt
import pytz
import functools
import itertools
import psycopg2
from sqlalchemy import exc


with open('../config/config.yaml', 'r') as f:
    CONFIG = yaml.load(f)

DSOURCE = dfu.Intrinio(verbose=False)

knowsec_creds = CONFIG['dbserver']
knowsec_creds['dbname'] = 'knowsec'
constr = database.constr_postgres(**knowsec_creds)
DBASE = database.StockDB(constr)

today = pytz.timezone('UTC').localize(dt.datetime.utcnow())
today = today.astimezone(pytz.timezone('US/Eastern'))
last_day = today - bus_day(n=1)
last_day = last_day.to_pydatetime()

IDX = pandas.IndexSlice


def utc(datetime):
    return pytz.timezone('UTC').localize(datetime)


def chunker(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def main():
    # # Full update exchange and securities table
    tables_to_update = ['exchanges', 'securities']
    exchanges = ['ARCX', 'NASDAQ', 'AMEX']
    for a_table in tables_to_update:
        rec_search = {'table': a_table, 'update_type': 'full'}
        current_update_rec = DBASE.retrieve_record('update_log', rec_search)

        should_update = False
        if current_update_rec is not None:
            udt = current_update_rec.reset_index().iloc[[0]]['update_dt']
            udt = udt.tolist()[0]
            seconds_elapsed = (dt.datetime.utcnow() - udt).total_seconds()

            if seconds_elapsed > 604800:
                should_update = True

        else:
            should_update = True

        if should_update is True:
            current_creds = DSOURCE.used_credits

            if a_table is 'exchanges':
                newdata = DSOURCE.get_exchanges().reset_index()
                newdata = newdata.to_dict('records')
            elif a_table is 'securities':
                pdata = list()

                for exch in exchanges:
                    sec = DSOURCE.get_securities_list(exchange_symbol=exch)
                    pdata.append(sec)

                newdata = functools.reduce(lambda x, y: x.append(y), pdata)
                newdata = newdata.reset_index().to_dict('records')

            used_creds = DSOURCE.used_credits - current_creds

            DBASE.clear_table(a_table)
            DBASE.bulk_insert_records(a_table, newdata)

            update_record = {
                'table': a_table,
                'update_dt': utc(dt.datetime.utcnow()),
                'update_type': 'full',
                'used_credits': used_creds,
                'new_records': len(newdata),
                'deleted_records': len(newdata),
                'updated_records': 0
            }

            DBASE.insert_record('update_log', update_record)

            DBASE.commit()
    # Get last business day prices for exchanges
    exchange_prices = [DSOURCE.get_exchange_prices(i, last_day.isoformat())
                       for i in exchanges]
    exchange_prices = functools.reduce(lambda x, y: x.append(y),
                                       exchange_prices)
    exchange_prices.sort_index(inplace=True)
    # TODO generate code for saving this data into Amazon S3

    # For EOD Prices, we're mixing AlphaVantage and Intrinio
    # Hence we need to select certain columns and rename some to match
    prices_colrename = {
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'adj_close': 'adjusted_close',
        'split_ratio': 'split_coefficient',
        'ex_dividend': 'dividend_amount'
    }

    exchange_prices.rename(index={'ARCX': 'NYSE'}, columns=prices_colrename,
                           inplace=True)
    exchange_prices = exchange_prices.rename_axis(['symbol', 'exchange',
                                                   'date'])
    exchange_prices = exchange_prices[list(prices_colrename.values())]

    # Get all securities, not delisted
    securities = DBASE.slice_table('securities', ['symbol', 'exchange'],
                                   index_keys=False)
    securities = [i for i in securities.to_records(index=False)]

    # Reduce calls by groupby symbol
    securities.sort(key=lambda x: x[0])
    securities_groups = itertools.groupby(securities, lambda x: x[0])
    ngroups = len({i[0] for i in securities})

    # Further, to reduce time spend querying the database, we chunk the groups
    chunk_groups

    secpbar = tqdm.tqdm(securities_groups, ncols=100, total=ngroups)

    bckoff = backoff.jittered_backoff(120, verbose=False)
    sql_bckoff = backoff.jittered_backoff(120, verbose=False)
    nnew_prices = 0
    current_creds = DSOURCE.used_credits
    for symbol, sec_entry in secpbar:
        secpbar.set_description(f"{symbol}")
        price_log_update = {'symbol': symbol}
        new_eod_records = list()

        exchs = [i[1] for i in sec_entry]

        while True:
            try:
                existing_rec = DBASE.retrieve_record('prices_log',
                                                     {'symbol': symbol})
                sql_bckoff(False)
                break

            except exc.OperationalError:
                sql_waited = sql_bckoff(True)
                DBASE.rollback()

        # Four update scenarios:
        #   1. The 'prices_log' table is empty
        #   2. No record for that security exist in 'prices_log'
        #   3. The 'max_date' for security is equal to last business day
        #   4. The 'max_date' is less, but only 1 day difference
        #   5. The 'max_date' is less, but more than 1 day difference
        #
        # Scenario 1 and 2 can be seen as the same, 'existing_rec' would be
        # `None` object
        # Scenario 3 means skip this security
        # Scenario 4 means pull prices from `exchange_prices`
        # Scenario 5 means pull prices from `exchange_prices` and continue
        # to pull historicals

        if existing_rec is None:
            # TODO pull all historicals from AlphaVantage
            pass
        else:
            max_date = existing_rec.to_dict('records')[0]['max_date']
            start_date = max_date + bus_day(n=1)
            start_date = start_date.to_pydatetime().date()

            if max_date >= last_day.date():
                pass

            elif max_date <= dt.date(2018, 8, 1):
                pass

            elif max_date == last_day.date():
                try:
                    new_rec = exchange_prices.loc[IDX['AMD'], :].iloc[[1]]
                    new_rec = new_rec.reset_index().to_dict('records')

                    new_eod_records.extend(new_rec)

                except KeyError:
                    pass

            elif max_date < last_day.date():
                end_date = last_day.date()

                while True:
                    try:
                        price_data = DSOURCE.get_prices(symbol, start_date.isoformat(),
                                                        end_date.isoformat())

                        bckoff(False)

                        break

                    except dfu.LimitError:
                        raise

                    except dfu.ServerError:
                        waited = bckoff(True)

                if price_data is None:
                    pass

                else:
                    price_data = price_data.rename(columns=prices_colrename)

                    price_data = price_data[['open', 'high', 'low',
                                             'close', 'volume', 'adjusted_close',
                                             'split_coefficient', 'dividend_amount']]

                    new_rec = price_data.reset_index().to_dict('records')
                    new_eod_records.extend(new_rec)

        check_time = pytz.timezone('UTC').localize(dt.datetime.utcnow())
        price_log_update['check_dt'] = check_time

        if new_eod_records:
            update_time = pytz.timezone('UTC').localize(dt.datetime.utcnow())
            price_log_update['update_dt'] = update_time

            for the_exchange in exchs:
                for record in new_eod_records:
                    record['exchange'] = the_exchange
                    # record['date'] = record['date'].date()

            while True:
                try:
                    DBASE.bulk_insert_records('security_prices',
                                              new_eod_records)
                    sql_bckoff(False)

                    DBASE.commit()

                    break

                except exc.IntegrityError:
                    sql_waited = sql_bckoff(True)
                    DBASE.rollback()

                    for new_record in new_eod_records:
                        try:
                            DBASE.insert_record('security_prices', new_record)

                            sql_bckoff(False)

                            DBASE.commit()

                        except exc.IntegrityError:
                            DBASE.rollback()

                    break

                except exc.OperationalError:
                    sql_waited = sql_bckoff(True)

                    DBASE.rollback()

            nnew_prices += len(new_eod_records)

            while True:
                try:
                    if existing_rec is None:
                        DBASE.insert_record('prices_log', price_log_update)
                    else:
                        DBASE.update_record('prices_log', price_log_update)

                    sql_bckoff(False)

                    DBASE.commit()

                    break

                except exc.OperationalError:
                    sql_waited = sql_bckoff(True)
                    DBASE.rollback()

        secpbar.set_postfix({'Credits': str(DSOURCE.used_credits)})


if __name__ == '__main__':
    main()
    sys.exit()
