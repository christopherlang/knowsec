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

    # Pull all securities for 'USCOMP' for latest business day ====

    # Pull reference tables ====
    all_securities = DBASE.slice_table('securities')
    all_tickers = set(all_securities.index.get_level_values(1))
    security_logs = DBASE.slice_table('prices_log')

    queries = query_generator(all_tickers, security_logs)

    # Loop setup --
    update_pb = tqdm.tqdm(queries, ncols=80, total=len(all_tickers))

    price_log_template = DBASE.table_columns('prices_log')
    price_log_template = {k: None for k in price_log_template}

    price_col_drop = ['security_id', 'company_id']
    # Loop setup end --

    for ticker, sdate, edate in update_pb:
        n_retries = 0
        skip_ticker = False

        found_sec = False
        new_rec_inserted = False

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

        update_log = price_log_template.copy()
        update_log['ticker'] = ticker
        update_log['check_dt'] = dateut.now_utc()

        sec_in_logs = security_logs.iloc[security_logs.index == ticker].empty
        sec_in_logs = not sec_in_logs

        # security_logs.iloc[security_logs.index == ticker]
        if eod_prices.empty is not True:
            found_sec = True
            update_log['ticker'] = eod_prices['ticker'].unique().item()

            eod_prices = eod_prices.drop(columns=price_col_drop)
            eod_price_recs = eod_prices.to_dict('records')

            for a_rec in eod_price_recs:
                if a_rec['volume'] is not None:
                    a_rec['volume'] = int(a_rec['volume'])

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
            update_log = {'ticker': ticker,
                          'check_dt': update_log['check_dt']}

            DBASE.update_record('prices_log', update_log)

        elif (found_sec and new_rec_inserted) and sec_in_logs is False:
            # Security retrieved new EOD, records were inserted, but security
            # does not exists in logs table
            where_statement = {'ticker': update_log['ticker']}
            ticker_data = DBASE.slice_table('security_prices',
                                            filters=where_statement,
                                            index_keys=False)

            update_log = {'ticker': ticker,
                          'min_date': ticker_data['date'].min(),
                          'max_date': ticker_data['date'].max(),
                          'update_dt': update_log['update_dt'],
                          'check_dt': update_log['check_dt']}

            DBASE.insert_record('prices_log', update_log)

        elif (found_sec and new_rec_inserted is False) and sec_in_logs is False:
            where_statement = {'ticker': update_log['ticker']}
            ticker_data = DBASE.slice_table('security_prices',
                                            filters=where_statement,
                                            index_keys=False)

            update_log = {'ticker': ticker,
                          'min_date': ticker_data['date'].min(),
                          'max_date': ticker_data['date'].max(),
                          'update_dt': None,
                          'check_dt': update_log['check_dt']}

            DBASE.insert_record('prices_log', update_log)

        # Update the prices_log table
        # if sec_in_logs is True:
        #     update_log = {k: v for k, v in update_log.items() if v is not None}
        #     DBASE.update_record('prices_log', update_log)

        # else:
        #     # If security not in 'prices_log' table, it should not be in the
        #     # 'security_prices' table either. Therefore, pass on the log update
        #     if eod_prices.empty is True:
        #         pass

        #     else:
        #         DBASE.insert_record('prices_log', update_log)

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
