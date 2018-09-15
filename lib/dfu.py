import requests
import re
import json
from datetime import datetime as dt
import pandas
import quandl
from bs4 import BeautifulSoup


def get_exchange(exchange="NASDAQ"):
    """ Stock symbols from NASDAQ

    Downloads, parses, and returns a Pandas DataFrame containing stock symbols
    for the NASDAQ, NYSE, and AMEX exchanges

    Arguements:
        exchange : str
            The exchange name. Can be one of 'NASDAQ', 'NYSE', or 'AMEX'

    Returns:
        a pandas.DataFrame object
    """
    url = "https://www.nasdaq.com/screening/companies-by-industry.aspx"
    payload = {'render': 'download'}

    if exchange == 'NASDAQ':
        payload['exchange'] = 'NASDAQ'
    elif exchange == 'NYSE':
        payload['exchange'] = 'NYSE'
    elif exchange == 'AMEX':
        payload['exchange'] = 'AMEX'
    else:
        raise ValueError("exchange parameter must be one of 'NASDAQ', "
                         "'NYSE', or 'AMEX'")

    symbols = requests.get(url, params=payload)

    symbols = symbols.text.split('\r\n')
    symbols = [re.sub(',$', '', i) for i in symbols]
    symbols = [re.split('["],["]', i) for i in symbols]

    for row in symbols:
        for i in range(len(row)):
            row[i] = row[i].replace('"', '')

            if row[i] == 'n/a':
                row[i] = None

    headers = symbols[0]
    symbols.pop(0)

    df_result = pandas.DataFrame(symbols, columns=headers)

    renamed_columns = {
        'ADR TSO': 'ADR_TSO',
        'Summary Quote': 'Summary_Quote'
    }

    df_result = df_result.rename(index=str, columns=renamed_columns)

    df_result['LastSale'] = pandas.to_numeric(df_result['LastSale'])
    df_result['MarketCap'] = pandas.to_numeric(df_result['MarketCap'])
    df_result['IPOyear'] = pandas.to_numeric(df_result['IPOyear'])
    df_result['ExchangeListing'] = exchange
    df_result['Update_dt'] = dt.utcnow().isoformat()

    return df_result


def get_sp500():
    """Stock symbols in the S&P500

    Downloads, parses, and returns a Pandas DataFrame containing stock symbols
    for that are part of the S&P500 index

    Returns:
        a pandas.DataFrame object
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    symbols = requests.get(url)
    symbols = BeautifulSoup(symbols.text, 'html.parser')

    result_table = list()
    table_iter = list(symbols.find('table'))[1].children

    for row in table_iter:
        try:
            if row.find('th') is not None:
                result_table.append([i.text for i in row.find_all('th')])
            elif row.find('td') is not None:
                result_table.append([i.text for i in row.find_all('td')])

        except AttributeError:
            pass

    for row in result_table:
        for i in range(len(row)):
            if row[i] == '\n' or row[i] == '':
                row[i] = None

            try:
                row[i] = row[i].replace('\n', '')
            except AttributeError:
                # Mainly to bypass None objects, which don't have replace
                pass

    headers = result_table[0]
    headers = [re.sub('[[]\d[]]', '', i) for i in headers]
    result_table.pop(0)

    df_result = pandas.DataFrame(result_table, columns=headers)
    df_result = df_result[['Ticker symbol', 'Security', 'GICS Sector',
                           'GICS Sub Industry']]

    cols_rename = {
        'Security': 'Organization', 'GICS Sector': 'Sector',
        'GICS Sub Industry': 'Industry', 'Ticker symbol': 'Symbol'
    }
    df_result = df_result.rename(index=str, columns=cols_rename)

    return df_result


def get_cpi(startyear, endyear, series=None):
    """Consumer Price Index download

    Downloads, parses, and returns a Pandas DataFrame containing multiple
    Consumer Price Index (CPI) series from the Bureau of Labor Statistics

    Returns:
        a pandas.DataFrame object
    """
    if isinstance(series, str):
        series = [series]

    if series is None:
        series = ['CUUR0000SA0L1E', 'CWSR0000SA111211', 'SUUR0000SA0',
                  'PCU22112222112241', 'NDU1051111051112345', 'WPS141101',
                  'APU000070111', 'LIUR0000SL00019']

    headers = {'Content-type': 'application/json'}
    payload = {
        "seriesid": series,
        "startyear": startyear,
        "endyear": endyear
    }
    payload = json.dumps(payload)

    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                      data=payload,
                      headers=headers)

    json_data = json.loads(p.text)

    result = list()
    for series in json_data['Results']['series']:
        seriesId = series['seriesID']

        if series['data']:
            for row in series['data']:
                row['footnotes'] = None

            dfr = pandas.DataFrame(series['data'])

            dfr['monthn'] = dfr['period'].apply(lambda x: x.replace('M', ''))
            dfr['Period'] = dfr['year'] + "-" + dfr['monthn'] + "-01"
            dfr['Period'] = pandas.to_datetime(dfr['Period'])
            dfr = dfr.drop(['period', 'monthn'], axis=1)
            dfr = dfr.rename(index=str, columns={'periodName': 'MonthName'})
            dfr['value'] = dfr['value'].astype('float')
            dfr['SeriesID'] = seriesId

            result.append(dfr)

    for i in range(1, len(result)):
        result[0] = result[0].append(result[i], ignore_index=True)

    if 'latest' in result[0].columns:
        result[0] = result[0].drop('latest', axis=1)

    return result[0]


class TimeSeriesData:
    def __init__(self):
        self._startdatetime = dt.utcnow()
        self._sources = dict()
        self._sources['stocks'] = 'Alpha Vantage'
        self._source_metadata = {
            'Alpha Vantage': {
                'requests': 0,
                'apikey': 'ARH5UW8CMDRTXLDM',
                'last_request': None
            },
            'Quandl': {
                'request': 0,
                'apikey': None,
                'last_request': None
            }
        }

    @property
    def sources(self):
        return self._sources

    def set_source(self, sourcetype, source):
        self._sources[sourcetype] = source

    @property
    def source_metadata(self):
        return self._source_metadata

    def get_stockprices(self, symbol, **kwargs):
        result = None

        print(kwargs)

        if self._sources['stocks'] == 'Alpha Vantage':
            result = self._stocksource_alphavantage(symbol=symbol,
                                                    args=kwargs)

        elif self._sources['stocks'] == 'Quandl':
            qdb = kwargs['database']
            del kwargs['database']
            result = self._stocksource_quandl(symbol=symbol, database=qdb,
                                              **kwargs)

        return result

    def _stocksource_quandl(self, symbol, database, **kwargs):
        query_name = '/'.join([database, symbol])
        result = quandl.get(query_name,
                            apikey=self._source_metadata['Quandl']['apikey'],
                            **kwargs)
        self._source_metadata['Quandl']['requests'] += 1
        self._source_metadata['Quandl']['last_request'] = dt.utcnow()

        return result

    def _stocksource_alphavantage(self, symbol, args):
        functions = dict()
        functions['daily'] = 'TIME_SERIES_DAILY'
        functions['weekly'] = 'TIME_SERIES_WEEKLY'
        functions['monthly'] = 'TIME_SERIES_MONTHLY'
        functions['daily_adj'] = 'TIME_SERIES_DAILY_ADJUSTED'
        functions['weekly_adj'] = 'TIME_SERIES_WEEKLY_ADJUSTED'
        functions['monthly_adj'] = 'TIME_SERIES_MONTHLY_ADJUSTED'

        url = 'https://www.alphavantage.co/query'

        try:
            fun = args['fun']
        except KeyError:
            fun = 'daily'

        try:
            output = args['output']
        except KeyError:
            output = 'compact'

        params = {
            'function': functions[fun],
            'symbol': symbol,
            'apikey': 'ARH5UW8CMDRTXLDM',
            'outputsize': output
        }

        req = requests.get(url, params=params)
        self._source_metadata['Alpha Vantage']['requests'] += 1
        self._source_metadata['Alpha Vantage']['last_request'] = dt.utcnow()

        # req_metadata = dict()
        # req_metadata['encoding'] = req.apparent_encoding
        # req_metadata['headers'] = req.headers
        # req_metadata['is_redirect'] = req.is_redirect
        # req_metadata['request_reason'] = req.reason
        # req_metadata['status_code'] = req.status_code
        # req_metadata['url'] = req.url

        # api_metadata = req.json()['Meta Data']

        json_head = req.json().keys()
        ts_key = [i for i in json_head if i.lower().find('time series') != -1]
        ts_key = ts_key[0]

        ts_data = req.json()[ts_key]

        headers = [list(i.keys()) for i in ts_data.values()]
        column_headers = [item for sublist in headers for item in sublist]
        column_headers = list(set(column_headers))
        column_headers.sort()
        column_headers = [(i, re.sub('\d[.]\s', '', i))
                          for i in column_headers]

        ts_data_formed = dict()
        ts_data_formed['Symbol'] = list()

        ts_data_timestamp = list()
        for _, header in column_headers:
            ts_data_formed[header] = list()

        for date, row in ts_data.items():
            ts_data_formed['Symbol'].append(symbol)
            ts_data_timestamp.append(dt.strptime(date, '%Y-%m-%d'))

            for old_header, pd_header in column_headers:
                ts_data_formed[pd_header].append(row[old_header])

        column_headers = [i[1] for i in column_headers]
        column_headers.insert(0, 'Symbol')

        result_df = pandas.DataFrame(data=ts_data_formed,
                                     columns=column_headers)

        result_df.set_index(pandas.DatetimeIndex(ts_data_timestamp,
                                                 name='Date'),
                            inplace=True)

        for col in column_headers:
            if col == 'Symbol':
                result_df[col] = result_df[col].astype('str')
            elif col == 'volume':
                result_df[col] = result_df[col].astype('int64')
            else:
                result_df[col] = result_df[col].astype('float64')

        return result_df
