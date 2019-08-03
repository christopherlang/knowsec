import sys
import backoff
import yaml
import dfu
import database
import tqdm
import date_utils as dateut
import pytz
import datetime
from sqlalchemy.exc import IntegrityError
import easylog
import pandas

PVERSION = '0.0.0.9999'

# LG = easylog.Easylog(create_console=False)
# LG.add_filelogger('../log/dbupdate.log', True)

with open('../config/config.yaml', 'r') as f:
    CONFIG = yaml.load(f)

    DSOURCE = dfu.Intrinio2(CONFIG['intrinio']['api_key'])

    knowsec_creds = CONFIG['dbserver']
    knowsec_creds['dbname'] = 'knowsec'
    constr = database.constr_postgres(**knowsec_creds)
    DBASE = database.StockDB(constr)

TODAYET = dateut.lag(dateut.today())


def main():
    # main level objects for use ====
    backoff_wait = backoff.jittered_backoff(60, verbose=False)
    price_col_drop = ['ticker', 'company_id']

    # Update exchanges table ====
    # exchanges_tab = DSOURCE.get_exchanges()
    # exchanges_records = exchanges_tab.to_dict('records')
    # DBASE.replace_table('exchanges', exchanges_records)
    # DBASE.commit()

    # Update securities table ====
    # securities_tab = DSOURCE.get_securities()
    # securities_records = securities_tab.to_dict('records')
    # DBASE.replace_table('securities', securities_records)
    # DBASE.commit()

    # Pull reference tables ====
    all_securities = DBASE.slice_table('securities')
    all_sec_id = set(all_securities.index.get_level_values(0))
    security_logs = DBASE.slice_table('prices_log')

    queries = query_generator(all_sec_id, security_logs)
    queries = list(queries)
    n_today_query = sum([s == TODAYET and e == TODAYET for t, s, e in queries])

    if n_today_query > 0:
        # Pull all securities for 'USCOMP' for latest business day ====
        latest_prices = DSOURCE.get_prices_exchange(TODAYET)
        latest_prices = latest_prices.drop(columns=['exchange_mic'])

        print(f"# of latest business queries: {n_today_query}")

    else:
        latest_prices = None

    # Loop setup --
    update_pb = tqdm.tqdm(queries, ncols=80)

    price_log_template = DBASE.table_columns('prices_log')
    price_log_template = {k: None for k in price_log_template}

    # Loop setup end --

    for secid, sdate, edate in update_pb:
        n_retries = 0
        skip_ticker = False

        found_sec = False
        new_rec_inserted = False

        if sdate == TODAYET and edate == TODAYET:
            # TODO implement soon after getting last business day
            eod_prices = latest_prices[latest_prices['secid'] == secid]

        else:
            while True:
                try:
                    eod_prices = DSOURCE.security_price(secid, sdate, edate)

                    break

                except dfu.ApiException:
                    n_retries += 1

                    if n_retries >= CONFIG['general']['max_retries_intrinio']:
                        skip_ticker = True

                        break

                    else:
                        backoff_wait()

        if skip_ticker is True:
            continue

        update_log = price_log_template.copy()
        update_log['secid'] = secid
        update_log['check_dt'] = dateut.now_utc()

        try:
            sec_in_logs = security_logs.iloc[security_logs.index == secid].empty
            sec_in_logs = not sec_in_logs

        except AttributeError:
            sec_in_logs = False

        # security_logs.iloc[security_logs.index == ticker]
        if eod_prices.empty is not True:
            found_sec = True
            update_log['secid'] = eod_prices['secid'].unique().item()

            eod_prices = eod_prices.drop(columns=price_col_drop)
            eod_price_recs = eod_prices.to_dict('records')

            for a_rec in eod_price_recs:
                if (a_rec['volume'] is not None and
                        pandas.isna(a_rec['volume']) is False):
                    a_rec['volume'] = int(a_rec['volume'])
                elif pandas.isna(a_rec['volume']) is True:
                    a_rec['volume'] = None

            try:
                DBASE.bulk_insert_records('security_prices', eod_price_recs)
                DBASE.flush()

                new_rec_inserted = True

                if sec_in_logs is not True:
                    update_log['min_date'] = eod_prices['date'].min()

                update_log['max_date'] = eod_prices['date'].max()

            except IntegrityError:
                DBASE.rollback()
                max_date = None
                min_date = None

                for a_rec in eod_price_recs:
                    try:
                        DBASE.insert_record('security_prices', a_rec)
                        DBASE.flush()

                        new_rec_inserted = True

                        if max_date is not None:
                            max_date = max(max_date, a_rec['date'])

                        else:
                            max_date = a_rec['date']

                        if min_date is not None:
                            min_date = min(min_date, a_rec['date'])

                        else:
                            min_date = a_rec['date']

                    except IntegrityError:
                        DBASE.rollback()
                        continue

                if sec_in_logs is not True:
                    update_log['min_date'] = min_date

                update_log['max_date'] = max_date

            finally:
                DBASE.commit()
                update_log['update_dt'] = dateut.now_utc()

        else:
            # empty, no data found
            pass

        if (found_sec and new_rec_inserted) and sec_in_logs:
            # Security retrieved new EOD, records were inserted, and security
            # exists in the logs table
            update_log = {k: v for k, v in update_log.items() if v is not None}

            DBASE.update_record('prices_log', update_log)

        elif (found_sec and new_rec_inserted is False) and sec_in_logs:
            # Security retrieved new EOD, but no records inserted (Key errors)
            # and security exists in logs table
            update_log = {'secid': secid,
                          'check_dt': update_log['check_dt']}

            DBASE.update_record('prices_log', update_log)

        elif (found_sec and new_rec_inserted) and sec_in_logs is False:
            # Security retrieved new EOD, records were inserted, but security
            # does not exists in logs table
            where_statement = {'secid': update_log['secid']}
            ticker_data = DBASE.slice_table('security_prices',
                                            filters=where_statement,
                                            index_keys=False)

            if ticker_data is None:
                min_date_sec = eod_prices['date'].min()
                max_date_sec = eod_prices['date'].max()

            else:
                min_date_sec = ticker_data['date'].min()
                min_date_sec = ticker_data['date'].max()

            update_log = {'secid': secid,
                          'min_date': min_date_sec,
                          'max_date': min_date_sec,
                          'update_dt': update_log['update_dt'],
                          'check_dt': update_log['check_dt']}

            DBASE.insert_record('prices_log', update_log)

        elif ((found_sec and new_rec_inserted is False) and
                sec_in_logs is False):
            where_statement = {'ticker': update_log['ticker']}
            ticker_data = DBASE.slice_table('security_prices',
                                            filters=where_statement,
                                            index_keys=False)

            if ticker_data is None:
                min_date_sec = eod_prices['date'].min()
                max_date_sec = eod_prices['date'].max()

            else:
                min_date_sec = ticker_data['date'].min()
                min_date_sec = ticker_data['date'].max()

            update_log = {'secid': secid,
                          'min_date': min_date_sec,
                          'max_date': max_date_sec,
                          'update_dt': None,
                          'check_dt': update_log['check_dt']}

            DBASE.insert_record('prices_log', update_log)

        DBASE.commit()


def query_generator(tickers, sec_logs):
    for ticker in tickers:
        if sec_logs is not None:
            ticker_log = sec_logs.iloc[sec_logs.index == ticker]

        else:
            ticker_log = pandas.DataFrame()

        if ticker_log.empty is True:
            query = (ticker, dateut.lag(TODAYET, 50, 'year'), TODAYET)

        elif len(ticker_log) > 1:
            raise DuplicationError(f"{len(ticker_log)} records found")

        else:
            current_max_date = ticker_log['max_date'][ticker]
            query_min_date = dateut.lead(current_max_date, 1, 'busday')
            query = (ticker, query_min_date, TODAYET)

        if query[1] > query[2]:
            continue

        else:
            yield query


class DuplicationError(Exception):
    def __init__(self, message):
        self.message = message


if __name__ == '__main__':
    if dateut.is_business_day(TODAYET):
        if dateut.now().time() >= datetime.time(17, 0, 0):
            main()

    sys.exit()
