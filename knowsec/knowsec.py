import os
# import sys
import backoff
import yaml
import dfu
import database
import tqdm
import pandas
import datetime as dt


CONFIG = dict()
with open('../config/data_config.yaml', 'r') as f:
    CONFIG['data'] = yaml.load(f)

db = database.StockDB('../db/storage.db')

try:
    db.delete_table('eod_stockprices')

except database.NoTableError:
    pass

db.create_all_tables()

# Perform company table update
comp_table = list()
for exchange in ['NYSE', 'NASDAQ', 'AMEX']:
    comp_table.append(dfu.get_exchange(exchange))

comp_table = comp_table[0].append(comp_table[1]).append(comp_table[2])

db.update_company(comp_table)

ds = dfu.AlphaVantage()

stock_update_log = db.retrieve_security_log()
pdidx = pandas.IndexSlice

symbol_list = set([i[0] for i in comp_table.index.get_values()])
symbol_list = [i for i in symbol_list if i != '']

records_update = list()

backoff_fun = backoff.exponential_backoff(120, False)  # max wait is 2 minutes
pbar = tqdm.tqdm(symbol_list, ncols=100)
for sym in pbar:
    sym_in_log = False

    try:
        sym_update_log = stock_update_log.loc[[pdidx[sym]], :]
        sym_in_log = True

    except KeyError:
        sym_update_log = None

    from_dt = None
    if sym_update_log is None:
        output_type = 'full'

    else:
        try:
            from_dt = sym_update_log.loc[pdidx[sym], 'maximum_datetime']
            from_dt = from_dt.tolist()[0] + dt.timedelta(days=1)

        except KeyError:
            output_type = 'full'

    while True:
        series = None

        try:
            if from_dt is None or output_type == 'full':
                series = ds.retrieve_data(sym, output='full')

            else:
                series = ds.retrieve_latest(sym, from_dt)

            backoff_fun(False)
            break

        except (dfu.InvalidCallError, dfu.GeneralCallError):
            break

        except (dfu.RateLimitError, dfu.requests.HTTPError):
            time_sleep = backoff_fun(True)['wait_time']
            pbar.write(f'Sleeping for {time_sleep:.2f} seconds')

    if series is not None:
        series = dfu.standardize_stock_series(series)
        db.bulk_insert_records('eod_stockprices', series)

        series_dt = [i[1] for i in series.index.get_values()]

        series_log = {
            'Symbol': sym,
            'minimum_datetime': min(series_dt).to_pydatetime(),
            'maximum_datetime': max(series_dt).to_pydatetime(),
            'update_dt': dt.datetime.utcnow()
        }

        if sym_in_log is True:
            # TODO implement update method in database.StockDB
            pass
        else:
            series_log['minimum_datetime'] = min(series_dt).to_pydatetime()
            db.insert_record('eod_stockprices_update_log', series_log)

    db.commit()
