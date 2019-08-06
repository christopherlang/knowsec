import os
import sys
import backoff
import yaml
import dfu
import database
import tqdm
import date_utils as dateut
import datetime
from sqlalchemy.exc import IntegrityError
import easylog
import pandas
import timetrack
import boto3

PVERSION = '0.0.0.9999'

with open('../config/config.yaml', 'r') as f:
    CONFIG = yaml.load(f, Loader=yaml.SafeLoader)

    DSOURCE = dfu.Intrinio2(CONFIG['intrinio']['api_key'])

    knowsec_creds = CONFIG['dbserver']
    knowsec_creds['dbname'] = 'knowsec'
    constr = database.constr_postgres(**knowsec_creds)
    DBASE = database.StockDB(constr)

    s3_access_id = CONFIG['S3']['aws_access_key_id']
    s3_secret_key = CONFIG['S3']['aws_secret_access_key']
    AWSS3 = boto3.client('s3', aws_access_key_id=s3_access_id,
                         aws_secret_access_key=s3_secret_key)

TODAYET = dateut.today()
IGNORETIME = CONFIG['general']['ignore_time']
DBCOMMIT = CONFIG['general']['commit_database']


def main():
    # main level objects for use ====
    backoff_wait = backoff.jittered_backoff(60, verbose=False)
    price_col_drop = ['ticker', 'company_id']
    time_track = timetrack.Timetrack()

    # record template for 'security_prices' table
    secprice_template = DBASE.table_columns('prices_log')
    secprice_template = {k: None for k in secprice_template}

    # record template for 'update_log' table
    updatelog_template = DBASE.table_columns('update_log')
    updatelog_template = {k: None for k in updatelog_template}
    del updatelog_template['id']

    # Update exchanges table ====
    # time_track.new_time('table')
    # exchanges_tab = DSOURCE.get_exchanges()
    # exchanges_records = exchanges_tab.to_dict('records')
    # DBASE.replace_table('exchanges', exchanges_records)
    # DBASE.flush()
    # time_track.pause_time('table')

    # updatelog_exch = updatelog_template.copy()
    # updatelog_exch['table_name'] = 'exchanges'
    # updatelog_exch['start_datetime'] = time_track.get_start_time('table')
    # updatelog_exch['end_datetime'] = time_track.now('table')
    # updatelog_exch['elapsed_seconds'] = time_track.elapsed_seconds('table')
    # updatelog_exch['num_api_queries'] = TODO
    # updatelog_exch['num_api_requests'] =
    # updatelog_exch['num_new_records'] =
    # updatelog_exch['num_update_records'] =
    # updatelog_exch['num_insert_records'] =

    # DBASE.insert_record('update_log', updatelog_exch)
    # DBASE.flush()

    # Update securities table ====
    # time_track.new_time('table')
    # securities_tab = DSOURCE.get_securities()
    # securities_records = securities_tab.to_dict('records')
    # DBASE.replace_table('securities', securities_records)
    # DBASE.flush()
    # time_track.pause_time('table')

    # updatelog_sec = updatelog_template.copy()
    # updatelog_sec['table_name'] = 'securities'
    # updatelog_sec['start_datetime'] = time_track.get_start_time('table')
    # updatelog_sec['end_datetime'] = time_track.now('table')
    # updatelog_sec['elapsed_seconds'] = time_track.elapsed_seconds('table')
    # updatelog_sec['num_api_queries'] = TODO
    # updatelog_sec['num_api_requests'] =
    # updatelog_sec['num_new_records'] =
    # updatelog_sec['num_update_records'] =
    # updatelog_sec['num_insert_records'] =

    # DBASE.insert_record('update_log', updatelog_sec)
    # DBASE.flush()

    # Pull reference tables ====
    all_securities = DBASE.slice_table('securities')
    all_sec_id = set(all_securities.index.get_level_values(0))
    security_logs = DBASE.slice_table('prices_log')

    time_track.new_time('price')
    updatelog_pri = updatelog_template.copy()
    updatelog_pri['table_name'] = 'security_prices'
    updatelog_pri['start_datetime'] = time_track.get_start_time('price')
    updatelog_pri['num_api_requests'] = 0
    updatelog_pri['num_new_records'] = 0
    updatelog_pri['num_update_records'] = 0
    updatelog_pri['num_insert_records'] = 0

    queries = query_generator(all_sec_id, security_logs)
    queries = list(queries)
    LG.log_info(f"# of securities and queries {len(queries)}")
    updatelog_pri['num_api_queries'] = len(queries)

    n_today_query = sum([s == TODAYET and e == TODAYET for t, s, e in queries])
    LG.log_info(f"# of latest business day queries {n_today_query}")

    if n_today_query > 0:
        # Pull all securities for 'USCOMP' for latest business day ====
        latest_prices = DSOURCE.get_prices_exchange(TODAYET)
        latest_prices = latest_prices.drop(columns=['exchange_mic'])

    else:
        latest_prices = None

    # Loop setup --
    update_pb = tqdm.tqdm(queries, ncols=80)

    # Loop setup end --

    for secid, sdate, edate in update_pb:
        n_retries = 0
        skip_ticker = False

        found_sec = False
        new_rec_inserted = False

        update_log = secprice_template.copy()
        update_log['secid'] = secid
        update_log['check_dt'] = dateut.now_utc()

        LG.log_info(f"security::({secid}) executing. "
                    f"Start date::({sdate}), End date::({edate})")

        if sdate == TODAYET and edate == TODAYET:
            eod_prices = latest_prices[latest_prices['secid'] == secid]
            LG.log_info(f"security::({secid}) queried for one business day")

        elif sdate > edate:
            skip_ticker = True
            LG.log_info(f"security::({secid}) skipped because start date is "
                        "after end date")

        else:
            while True:
                try:
                    updatelog_pri['num_api_requests'] += 1
                    eod_prices = DSOURCE.security_price(secid, sdate, edate)
                    LG.log_info(f"security::({secid}) API request returned")
                    break

                except dfu.ApiException:
                    n_retries += 1
                    LG.log_info(f"security '{secid}' retried {n_retries}")

                    if n_retries >= CONFIG['general']['max_retries_intrinio']:
                        skip_ticker = True
                        LG.log_info(f"security::({secid}) skipped because API "
                                    "requests has reached max retries")
                        break

                    else:
                        backoff_wait()

        if skip_ticker is True:
            update_log = {k: v for k, v in update_log.items() if v is not None}

            if secid in security_logs.index:
                DBASE.update_record('prices_log', update_log)

            continue

        try:
            sec_in_logs = security_logs.iloc[security_logs.index == secid]
            sec_in_logs = sec_in_logs.empty
            sec_in_logs = not sec_in_logs

        except AttributeError:
            sec_in_logs = False

        # security_logs.iloc[security_logs.index == ticker]
        if eod_prices.empty is not True:
            found_sec = True
            updatelog_pri['num_new_records'] += len(eod_prices)

            LG.log_info(f"security::({secid}) has {len(eod_prices)} new recs")

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

                LG.log_info(f"security::({secid}) bulk insert successful")

                updatelog_pri['num_insert_records'] += len(eod_price_recs)
                new_rec_inserted = True

                if sec_in_logs is not True:
                    update_log['min_date'] = eod_prices['date'].min()

                update_log['max_date'] = eod_prices['date'].max()

            except IntegrityError:
                DBASE.rollback()
                LG.log_info(f"security::({secid}) failed bulk inserts due to "
                            "IntegrityError. Will try individual record "
                            "inserts")
                max_date = None
                min_date = None

                for a_rec in eod_price_recs:
                    try:
                        DBASE.insert_record('security_prices', a_rec)
                        DBASE.flush()

                        updatelog_pri['num_insert_records'] += 1
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
                        LG.log_info(f"security::({secid}) "
                                    "date::({a_rec['date']}) failed to insert")
                        continue

                if sec_in_logs is not True:
                    update_log['min_date'] = min_date

                update_log['max_date'] = max_date

            finally:
                DBASE.flush()
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

        DBASE.flush()

    time_track.pause_time('price')
    updatelog_pri['end_datetime'] = time_track.now('price')
    updatelog_pri['elapsed_seconds'] = time_track.elapsed_seconds('price')
    DBASE.insert_record('update_log', updatelog_pri)

    DBASE.flush()


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

        yield query


class DuplicationError(Exception):
    def __init__(self, message):
        super().__init__()
        self.message = message


if __name__ == '__main__':
    if dateut.is_business_day(TODAYET):
        curr_time = dateut.now().time()
        fivepm = datetime.time(17)
        b_midnight = datetime.time(23, 59, 59)
        a_midnight = datetime.time(0, 0, 1)
        fiveam = datetime.time(5)

        if ((curr_time >= fivepm and curr_time < b_midnight) or
                (curr_time >= a_midnight and curr_time < fiveam) or
                IGNORETIME):

            LG = easylog.Easylog(create_console=False)
            LG.add_filelogger('../log/knowsec.log')

            LG.log_info(dateut.now_utc())

            try:
                main()

                LG.log_info("main execution completed")

                logpath = LG.logfile[0]['filename']
                log_filename = os.path.basename(logpath)
                s3_path = 'knowsec/dbupdate_logs/'
                s3_path += os.path.basename(log_filename)

                LG.log_info(f"saving log file '{logpath}' to S3 '{s3_path}'")

                AWSS3.upload_file(logpath, 'prometheus-project', s3_path)

                LG.log_info(f"removing log file {logpath}")
                os.remove(logpath)

            except Exception:
                # TODO capture error and output message to log file
                pass

            finally:
                if DBCOMMIT is True:
                    LG.log_info('committing changes on database')
                    DBASE.commit()

                LG.log_info('closing logger and database connections')
                LG.close()
                DBASE.close()

    sys.exit()
