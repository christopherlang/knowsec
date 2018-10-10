import pandas
import datetime as dt
import pytz


historicals = pandas.read_csv('../data/historicals.tsv', delimiter='\t')
historicals.reset_index(inplace=True)
historicals['Datetime'] = pandas.to_datetime(historicals['Datetime'])
historicals['Date'] = historicals['Datetime'].apply(lambda x: x.date())
historicals = historicals.drop('Datetime', axis=1)

securities = pandas.read_csv('../data/securities.tsv', sep='\t')
securities = securities[['Symbol', 'Exchange']].drop_duplicates()
securities = securities.set_index('Symbol')

historicals = historicals.join(securities, on='Symbol', how='inner')
historicals = historicals.rename(columns={'Symbol': 'symbol', 'Exchange': 'exchange', 'Date': 'date'})
historicals = historicals.set_index(['symbol', 'exchange', 'date'])

historicals = historicals.drop('index', axis=1)

cols_to_float = ['open', 'high', 'low', 'close', 'adjusted_close', 'dividend_amount', 'split_coefficient']

historicals[cols_to_float] = historicals[cols_to_float].applymap(lambda x: x / 10000)

historicals = historicals[['open', 'high', 'low', 'close', 'volume', 'adjusted_close', 'dividend_amount', 'split_coefficient']]
historicals.to_csv('../data/historicals2.csv', sep=',', encoding='utf-8')
historicals.head(1000).to_csv('../data/historicals2_sample.csv', sep=',', encoding='utf-8')