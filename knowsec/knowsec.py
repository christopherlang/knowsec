import sys
import backoff
import yaml
import dfu
import database
import tqdm
import pandas
import datetime as dt
import functools


with open('../config/config.yaml', 'r') as f:
    CONFIG = yaml.load(f)


# DBASE = database.StockDB('../db/storage10.db')
DSOURCE_A = dfu.AlphaVantage()
DSOURCE_B = dfu.Intrinio(verbose=False)

knowsec_creds = CONFIG['dbserver']
knowsec_creds['dbname'] = 'knowsec'
constr = database.constr_postgres(**knowsec_creds)
DBASE = database.StockDB(constr)

prices = pandas.read_csv('../data/historicals2.csv', sep=',')
prices['Date'] = pandas.to_datetime(prices['Date'])

securities = pandas.read_csv('../data/securities.tsv', sep='\t')
securities = securities[['Symbol', 'Exchange']].drop_duplicates()
securities = securities.set_index('Symbol')

prices = prices.join(securities, on='Symbol', how='inner')

prices = prices.set_index(['Symbol', 'Exchange', 'Date'])

DBASE.bulk_insert_records('security_prices', prices.to_dict('records'))
# def main():
#     DSOURCE_B.get_exchanges().to_csv('../data/exchanges.tsv', sep='\t',
#                                      encoding='utf-8')

#     securities = [DSOURCE_B.get_securities_list(exchange_symbol=i)
#                   for i in ['ARCX', 'NASDAQ', 'AMEX']]

#     securities = functools.reduce(lambda pd1, pd2: pd1.append(pd2), securities)

#     securities.to_csv('../data/securities.tsv', sep='\t', encoding='utf-8')

#     symbol_list = {i[0] for i in securities.index.values}
#     symbol_list = list(symbol_list)
#     symbol_list.sort()

#     symbol_list = symbol_list[symbol_list.index('SDS'):]

#     backoff_fun = backoff.jittered_backoff(120, verbose=False)
#     pbar = tqdm.tqdm(symbol_list, ncols=100)

#     for sym in pbar:
#         series = None

#         while True:
#             try:
#                 pbar.write(f'Data for symbol: {sym}')
#                 series = DSOURCE_A.retrieve_data(sym, output='full')

#                 backoff_fun(False)

#                 pbar.write(f'{sym} API call was successful')

#                 try:
#                     series.to_csv('../data/historicals/' + sym + '.tsv', sep='\t',
#                                   encoding='utf-8')
#                 except FileNotFoundError as e:
#                     print(e)
#                     print(series)

#                 break

#             except (dfu.InvalidCallError, dfu.GeneralCallError):
#                 pbar.write(f'Data for symbol: {sym} has failed, skipping')
#                 break

#             except (dfu.RateLimitError, dfu.requests.HTTPError):
#                 time_sleep = backoff_fun(True)['wait_time']
#                 pbar.write(f'Sleeping for {time_sleep:.2f} seconds')

# def main():
#     DBASE.create_all_tables()

#     # Perform company table update
#     comp_table = update_securities_table()

#     stock_update_log = DBASE.retrieve_security_log()
#     pdidx = pandas.IndexSlice

#     symbol_list = set([i[0] for i in comp_table.index.get_values()])
#     symbol_list = [i for i in symbol_list if i != '']

#     backoff_fun = backoff.exponential_backoff(120, False)
#     pbar = tqdm.tqdm(symbol_list, ncols=100)
#     for sym in pbar:
#         # Determine first if it needs a full update, or partial update
#         # Done through the 'stock_update_log'
#         # If symbol does not exist, or if 'stock_update_log' is None,
#         # do full update. Otherwise, partial
#         try:
#             sym_update_log = stock_update_log.loc[[pdidx[sym]], :]
#             sym_in_log = True

#         except KeyError:
#             sym_update_log = None
#             sym_in_log = False

#         perform_full = sym_in_log is not True or len(stock_update_log) == 0

#         if perform_full is True:
#             update_log = False

#             get_data = functools.partial(DSOURCE.retrieve_data, output='full')

#         else:
#             update_log = True

#             from_dt = sym_update_log.loc[pdidx[sym], 'maximum_datetime']
#             from_dt = from_dt.to_pydatetime() + dt.timedelta(days=1)

#             if from_dt < dt.datetime.utcnow():
#                 get_data = functools.partial(DSOURCE.retrieve_latest,
#                                              from_dt=from_dt)

#             else:
#                 continue

#         series = None
#         break_
#         while True:
#             try:
#                 series = get_data(sym)
#                 backoff_fun(False)
#                 break

#             except (dfu.InvalidCallError, dfu.GeneralCallError):
#                 break

#             except (dfu.RateLimitError, dfu.requests.HTTPError):
#                 time_sleep = backoff_fun(True)['wait_time']
#                 pbar.write(f'Sleeping for {time_sleep:.2f} seconds')

#         if series is not None and series.empty is not True:
#             series = dfu.standardize_stock_series(series)

#             pbar.write(f"Bulk insert '{sym}', # {len(series)} records")
#             DBASE.bulk_insert_records('eod_stockprices', series)
#             DBASE.commit()

#             series_dt = [i[1] for i in series.index.get_values()]

#             if update_log is True:
#                 # TODO actually implement this
#                 # Record already exist in symbol log, update max time
#                 # series_log = {
#                 #     'Symbol': sym,
#                 #     'maximum_datetime': max(series_dt).to_pydatetime(),
#                 #     'update_dt': dt.datetime.utcnow()
#                 # }
#                 pass

#             else:
#                 # This is a new log record. Insert a new row
#                 series_log = {
#                     'Symbol': sym,
#                     'minimum_datetime': min(series_dt).to_pydatetime(),
#                     'maximum_datetime': max(series_dt).to_pydatetime(),
#                     'update_dt': dt.datetime.utcnow()
#                 }

#                 series_log['minimum_datetime'] = min(series_dt).to_pydatetime()
#                 DBASE.insert_record('eod_stockprices_update_log', series_log)
#                 DBASE.commit()


# def update_securities_table():
#     """Updates the 'securities' table in database

#     HAS SIDE EFFECTS. Will modify/update the 'securities' table

#     Returns
#     -------
#     Pandas dataframe, containing all securities
#     """

#     comp_table = list()
#     for exchange in ['NYSE', 'NASDAQ', 'AMEX', 'ETF']:
#         comp_table.append(dfu.download_symbols(exchange))

#     comp_table = (
#         comp_table[0].
#         append(comp_table[1]).
#         append(comp_table[2]).
#         append(comp_table[3])
#     )

#     DBASE.update_securities(comp_table)

#     DBASE.commit()

#     return comp_table


if __name__ == '__main__':
    main()
    sys.exit()
