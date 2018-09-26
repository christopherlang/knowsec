import sys
import backoff
import yaml
import dfu
import database
import tqdm
import pandas
import datetime as dt
import functools


CONFIG = dict()
with open('../config/data_config.yaml', 'r') as f:
    CONFIG['data'] = yaml.load(f)

DBASE = database.StockDB('../db/storage4.db')
DSOURCE = dfu.AlphaVantage()


def main():
    DBASE.create_all_tables()

    # Perform company table update
    comp_table = update_securities_table()

    stock_update_log = DBASE.retrieve_security_log()
    pdidx = pandas.IndexSlice

    symbol_list = set([i[0] for i in comp_table.index.get_values()])
    symbol_list = [i for i in symbol_list if i != '']

    backoff_fun = backoff.exponential_backoff(120, False)
    pbar = tqdm.tqdm(symbol_list, ncols=100)
    for sym in pbar:
        # Determine first if it needs a full update, or partial update
        # Done through the 'stock_update_log'
        # If symbol does not exist, or if 'stock_update_log' is None,
        # do full update. Otherwise, partial
        try:
            sym_update_log = stock_update_log.loc[[pdidx[sym]], :]
            sym_in_log = True

        except KeyError:
            sym_update_log = None
            sym_in_log = False

        perform_full = sym_in_log is not True or len(stock_update_log) == 0

        if perform_full is True:
            update_log = False

            get_data = functools.partial(DSOURCE.retrieve_data, output='full')

        else:
            update_log = True

            from_dt = sym_update_log.loc[pdidx[sym], 'maximum_datetime']
            from_dt = from_dt.to_pydatetime() + dt.timedelta(days=1)

            if from_dt < dt.datetime.utcnow():
                get_data = functools.partial(DSOURCE.retrieve_latest,
                                             from_dt=from_dt)

            else:
                continue

        series = None
        break_
        while True:
            try:
                series = get_data(sym)
                backoff_fun(False)
                break

            except (dfu.InvalidCallError, dfu.GeneralCallError):
                break

            except (dfu.RateLimitError, dfu.requests.HTTPError):
                time_sleep = backoff_fun(True)['wait_time']
                pbar.write(f'Sleeping for {time_sleep:.2f} seconds')

        if series is not None and series.empty is not True:
            series = dfu.standardize_stock_series(series)

            pbar.write(f"Bulk insert '{sym}', # {len(series)} records")
            DBASE.bulk_insert_records('eod_stockprices', series)
            DBASE.commit()

            series_dt = [i[1] for i in series.index.get_values()]

            if update_log is True:
                # TODO actually implement this
                # Record already exist in symbol log, update max time
                # series_log = {
                #     'Symbol': sym,
                #     'maximum_datetime': max(series_dt).to_pydatetime(),
                #     'update_dt': dt.datetime.utcnow()
                # }
                pass

            else:
                # This is a new log record. Insert a new row
                series_log = {
                    'Symbol': sym,
                    'minimum_datetime': min(series_dt).to_pydatetime(),
                    'maximum_datetime': max(series_dt).to_pydatetime(),
                    'update_dt': dt.datetime.utcnow()
                }

                series_log['minimum_datetime'] = min(series_dt).to_pydatetime()
                DBASE.insert_record('eod_stockprices_update_log', series_log)
                DBASE.commit()


def update_securities_table():
    """Updates the 'securities' table in database

    HAS SIDE EFFECTS. Will modify/update the 'securities' table

    Returns
    -------
    Pandas dataframe, containing all securities
    """

    comp_table = list()
    for exchange in ['NYSE', 'NASDAQ', 'AMEX', 'ETF']:
        comp_table.append(dfu.download_symbols(exchange))

    comp_table = (
        comp_table[0].
        append(comp_table[1]).
        append(comp_table[2]).
        append(comp_table[3])
    )

    DBASE.update_securities(comp_table)

    DBASE.commit()

    return comp_table


if __name__ == '__main__':
    main()
    sys.exit()
