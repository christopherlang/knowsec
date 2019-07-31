import sys
import backoff
import yaml
import dfu
import database
import tqdm
import date_utils as dateut
import pytz
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

TODAYET = dateut.today()


def main():
    # main level objects for use ====
    backoff_wait = backoff.jittered_backoff(60, verbose=False)

    all_securities = DBASE.slice_table('securities')
    all_tickers = set(all_securities.index.get_level_values(1))
    security_logs = DBASE.slice_table('prices_log')

    queries = query_generator(all_tickers, security_logs)

    price_log_template = DBASE.table_columns('prices_log')
    price_log_template = {k: None for k in price_log_template}

    update_pb = tqdm.tqdm(queries, ncols=80, total=len(all_tickers))
    for ticker, sdate, edate in update_pb:
        n_retries = 0
        skip_ticker = False

        while True:
            try:
                eod_prices = DSOURCE.security_price(ticker, sdate, edate)
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

        eod_price_recs = eod_prices.to_dict('records')

        update_log = price_log_template.copy()
        update_log['ticker'] = ticker
        update_log['check_dt'] = dateut.now_utc()

        # security_logs.iloc[security_logs.index == ticker]
        if eod_prices.empty is not True:
            for a_rec in eod_price_recs:
                if a_rec['volume'] is not None:
                    a_rec['volume'] = int(a_rec['volume'])

            try:
                DBASE.bulk_insert_records('security_prices', eod_price_recs)
                DBASE.flush()
                update_log['max_date'] = eod_prices['date'].max()

            except IntegrityError:
                DBASE.rollback()
                max_date = None

                for a_rec in eod_price_recs:
                    try:
                        DBASE.insert_record('security_prices', a_rec)
                        DBASE.flush()

                        if max_date is not None:
                            max_date = max(max_date, a_rec['date'])

                        else:
                            max_date = a_rec['date']

                    except IntegrityError:
                        DBASE.rollback()
                        continue

                update_log['max_date'] = max_date

            finally:
                DBASE.commit()
                update_log['update_dt'] = dateut.now_utc()

        else:
            # empty, no data found
            pass

        # Update the prices_log table
        update_log = {k: v for k, v in update_log.items() if v is not None}
        sec_in_logs = security_logs.iloc[security_logs.index == ticker].empty
        sec_in_logs = not sec_in_logs
        if sec_in_logs is True:
            DBASE.update_record('prices_log', update_log)

        else:
            # If security not in 'prices_log' table, it should not be in the
            # 'security_prices' table either. Therefore, pass on the log update
            if eod_prices.empty is True:
                pass

            else:
                DBASE.insert_record('prices_log', update_log)

        DBASE.commit()


def query_generator(tickers, sec_logs):
    for ticker in tickers:
        ticker_log = sec_logs.iloc[sec_logs.index == ticker]

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