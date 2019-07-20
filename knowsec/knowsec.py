import sys
import backoff
import yaml
import dfu
import database
import tqdm
import pandas
from pandas.tseries.offsets import BDay as bus_day
import datetime as dt
import pytz
import functools
import itertools
import psycopg2
from sqlalchemy import exc
import easylog

PVERSION = '0.0.0.9999'

LG = easylog.Easylog(create_console=False)
LG.add_filelogger('../log/dbupdate.log', True)

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

LG.log_info(f"last complete business day is {last_day.date().isoformat()}")

IDX = pandas.IndexSlice


def utc(datetime):
    return pytz.timezone('UTC').localize(datetime)


def chunker(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def main():
    # Add in some header info for log file
    LG.log_info(f'knowsec version: {PVERSION}')
    LG.log_info(f'Date and time: {dt.datetime.utcnow().isoformat()}')

    # # Full update exchange and securities table
    tables_to_update = ['exchanges', 'securities']
    exchanges = ['ARCX', 'NYSE', 'NASDAQ', 'AMEX']

    LG.log_info(f'Tables to update: {tables_to_update}')
    LG.log_info(f'Exchanges to update: {exchanges}')

    for a_table in tables_to_update:
        LG.log_info(f'Updating table {a_table}')

        rec_search = {'table': a_table, 'update_type': 'full'}
        current_update_rec = DBASE.retrieve_record('update_log', rec_search)

        should_update = False
        if current_update_rec is not None:
            LG.log_info(f'Table {a_table} found in database')
            LG.log_info(f'# of records in table {a_table}: {len(current_update_rec)}')

            udt = current_update_rec.reset_index().iloc[[0]]['update_dt']
            udt = udt.tolist()[0]
            seconds_elapsed = (dt.datetime.utcnow() - udt).total_seconds()

            if seconds_elapsed > 604800:
                should_update = True

        else:
            LG.log_info(f'Table {a_table} not found in database')
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

            LG.log_info(f'# of new records in new data: {len(newdata)}')

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
    LG.log_info(f'Download last business day prices')

    exchange_prices = [DSOURCE.get_exchange_prices(i, last_day.isoformat())
                       for i in exchanges]

    if all([i is None for i in exchange_prices]):
        raise TypeError('Last business day failed to retrieve prices')

    exchange_prices = functools.reduce(lambda x, y: x.append(y),
                                       exchange_prices)
    exchange_prices.sort_index(inplace=True)

    LG.log_info(f'# of new records in last business day prices: {len(exchange_prices)}')
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

    # Further, to reduce time spend querying the database, we cache results
    # for insertion and pull in whole prices_log
    price_cache = list()
    existing_securities = DBASE.slice_table('prices_log')

    secpbar = tqdm.tqdm(securities_groups, ncols=100, total=ngroups)

    bckoff = backoff.jittered_backoff(120, verbose=False)
    sql_bckoff = backoff.jittered_backoff(120, verbose=False)
    nnew_prices = 0
    current_creds = DSOURCE.used_credits

    LG.log_info(f'Starting EOD price download and upload')
    LG.log_info(f'# of used credits since: {current_creds}')

    for symbol, sec_entry in secpbar:
        LG.log_info(f'Symbol being looked into: "{symbol}"')

        secpbar.set_description(f"'{symbol}'")
        price_log_update = {'symbol': symbol}
        new_eod_records = list()

        exchs = [i[1] for i in sec_entry]
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

        try:
            existing_rec = existing_securities.loc[IDX[[symbol]], :]
            LG.log_info(f'"{symbol}" already exists in prices table')
        except KeyError:
            existing_rec = None
            LG.log_info(f'"{symbol}" does not exists in prices table')

        if existing_rec is None:
            # TODO pull all historicals from AlphaVantage
            pass
        else:
            max_date = existing_rec.to_dict('records')[0]['max_date']
            start_date = max_date + bus_day(n=1)
            start_date = start_date.to_pydatetime().date()

            if start_date >= last_day.date():
                log_msg = f"'{symbol}' has max date {max_date}"
                log_msg += f"', which is greater or equal to "
                log_msg += f"{last_day.date().isoformat()}"

                LG.log_info(log_msg)
                pass

            elif start_date <= dt.date(2018, 8, 1):
                log_msg = f"'{symbol}' has max date {max_date}"
                log_msg += f"', which is less or equal to 2018-08-01"

                LG.log_info(log_msg)
                pass

            elif start_date == last_day.date():
                try:
                    new_rec = exchange_prices.loc[IDX['AMD'], :].iloc[[1]]
                    new_rec = new_rec.reset_index().to_dict('records')

                    new_eod_records.extend(new_rec)

                    log_msg = f"'{symbol}' has max date {max_date}"
                    log_msg += f"', which is equal to "
                    log_msg += f"{last_day.date().isoformat()}"

                    LG.log_info(log_msg)
                    LG.log_info("Should only pull from 'exchange_prices'")

                except KeyError:
                    pass

            elif start_date < last_day.date():
                end_date = last_day.date()

                log_msg = f"'{symbol}' has max date {max_date}"
                log_msg += f"' less than {last_day.date().isoformat()}"
                LG.log_info(log_msg)
                LG.log_info(f"Attempting to download prices for '{symbol}'")

                while True:
                    try:
                        price_data = DSOURCE.get_prices(symbol,
                                                        start_date.isoformat(),
                                                        end_date.isoformat())

                        log_msg = f"Successfully downloaded prices for"
                        log_msg += f" '{symbol}'"
                        LG.log_info(log_msg)

                        bckoff(False)

                        break

                    except dfu.LimitError:
                        break

                    except dfu.ServerError:
                        waited = bckoff(True)

                if price_data is None:
                    log_msg = f"No data found from API for '{symbol}'"
                    LG.log_info(log_msg)

                else:
                    LG.log_info(f'# of new records: {len(price_data)}')

                    price_data = price_data.rename(columns=prices_colrename)

                    price_data = price_data[['open', 'high', 'low',
                                             'close', 'volume',
                                             'adjusted_close',
                                             'split_coefficient',
                                             'dividend_amount']]

                    new_rec = price_data.reset_index().to_dict('records')
                    new_eod_records.extend(new_rec)

        check_time = pytz.timezone('UTC').localize(dt.datetime.utcnow())
        price_log_update['check_dt'] = check_time

        if new_eod_records:
            log_msg = f"'{symbol}': found {len(new_eod_records)} new records"
            LG.log_info(log_msg)

            update_time = pytz.timezone('UTC').localize(dt.datetime.utcnow())
            price_log_update['update_dt'] = update_time
            price_log_update['max_date'] = start_date

            for the_exchange in exchs:
                for record in new_eod_records:
                    record['exchange'] = the_exchange
                    # record['date'] = record['date'].date()

            while True:
                try:
                    log_msg = f"'{symbol}': attemping bulk insert record"
                    LG.log_info(log_msg)

                    DBASE.bulk_insert_records('security_prices',
                                              new_eod_records)
                    sql_bckoff(False)

                    DBASE.commit()

                    break

                except exc.IntegrityError:
                    log_msg = f"'{symbol}': encountered integrity error"
                    LG.log_info(log_msg)

                    sql_waited = sql_bckoff(True)
                    DBASE.rollback()

                    log_msg = f"'{symbol}': attempting single record inserts"
                    LG.log_info(log_msg)
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
