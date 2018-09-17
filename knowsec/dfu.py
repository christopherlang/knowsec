import requests
import re
import json
from datetime import datetime as dt
import pandas
import pytz
from bs4 import BeautifulSoup
from abc import ABCMeta, abstractmethod
import numpy as np


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
    df_result['Update_dt'] = pytz.timezone('UTC').localize(dt.utcnow())

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

    def get_quote(self, symbols):
        if isinstance(symbols, str):
            symbolList = [symbols]
        else:
            symbolList = symbols

            if len(symbolList) > 25:
                symbolList = symbolList[0:25]

        symbolList = ','.join(symbolList)

        url = 'https://marketdata.websol.barchart.com/getQuote.json'
        params = {
            'apikey': 'edfaca2b49e5b010c2c39298a89a37ac',
            'symbols': symbolList
        }

        req = requests.get(url, params=params)
        symbol_quotes = req.json()['results']
        symbol_quotes = [i for i in symbol_quotes if i['mode'] == 'd']

        eastern_tz = pytz.timezone('US/Eastern')

        for a_quote in symbol_quotes:
            a_quote['Datetime'] = a_quote['tradeTimestamp'][0:19]
            a_quote['Datetime'] = dt.strptime(a_quote['Datetime'],
                                              '%Y-%m-%dT%H:%M:%S')
            a_quote['Datetime'] = (
                eastern_tz.localize(a_quote['Datetime'])
                .astimezone(pytz.timezone('UTC'))
            )

        quotes = (
            pandas.DataFrame.from_dict(symbol_quotes)
            .filter(items=['symbol', 'Datetime', 'open',
                           'high', 'low', 'close', 'volume'])
            .rename(index=str, columns={'symbol': 'Symbol'})
            .set_index(['Symbol', 'Datetime'])
        )

        return quotes

    def get_stockprices(self, symbol, **kwargs):
        result = None

        print(kwargs)

        if self._sources['stocks'] == 'Alpha Vantage':
            result = self._stocksource_alphavantage(symbol=symbol,
                                                    args=kwargs)

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
        self._source_metadata['Alpha Vantage']['last_request'] = (
            pytz.timezone('UTC').localize(dt.utcnow())
        )

        # Get time zone
        data_tz = req.json()['Meta Data'].keys()
        data_tz = [i for i in data_tz if i.lower().find('time zone') != -1][0]
        data_tz = req.json()['Meta Data'][data_tz]
        data_tz = pytz.timezone(data_tz)

        json_head = req.json().keys()
        ts_key = [i for i in json_head if i.lower().find('time series') != -1]
        ts_key = ts_key[0]

        ts_data = req.json()[ts_key]

        colheaders = [list(i.keys()) for i in ts_data.values()]
        colheaders = [item for sublist in colheaders for item in sublist]
        colheaders = list(set(colheaders))
        colheaders.sort()
        colrenames = {k: re.sub('\d[.]\s', '', k) for k in colheaders}

        for date, row in ts_data.items():
            for row_element_name in row:
                if row_element_name.find('volume') != -1:
                    row[row_element_name] = int(row[row_element_name])
                else:
                    row[row_element_name] = float(row[row_element_name])

            row['Datetime'] = data_tz.localize(dt.strptime(date, '%Y-%m-%d'))
            row['Datetime'] = row['Datetime'].astimezone(pytz.timezone('UTC'))

            row['Symbol'] = symbol

        ts_data = [i for i in ts_data.values()]

        histprice = (
            pandas.DataFrame.from_dict(ts_data)
            .set_index(['Symbol', 'Datetime'])
            .rename(index=str, columns=colrenames)
        )

        return histprice

    def _request_metadata(self, req):
        req_metadata = dict()
        req_metadata['encoding'] = req.apparent_encoding
        req_metadata['headers'] = req.headers
        req_metadata['is_redirect'] = req.is_redirect
        req_metadata['request_reason'] = req.reason
        req_metadata['status_code'] = req.status_code
        req_metadata['url'] = req.url
        req_metadata['api_meta'] = req.json()['Meta Data']

        return req_metadata


class DataSource(metaclass=ABCMeta):
    """Parent class defining a standardized data source API

    Classes that inherit this metaclass will have standardized properties and
    methods useful to retrieve data and get information about the data source
    itself, such as the name, api keys (if applicable), request logs, etc.

    The primary way to interact with classes that inherit this metaclass is the
    abstract method `get_data`, with parameters as needed. This method should
    return `pandas.core.frame.DataFrame` whenever possible. Other structures
    are allowed where when needed however. The output data structure, types,
    and others must be explicitly described in the method's docstring

    In child class make sure you `super().__init__()` before the class
    instantiates its own properties

    Standard Naming for Retrieval
    -----------------------------
    Method names for retrieving resource should adhere to the following:
        - All lowercase whenever possible
        - Max three words, delimited by '_'
        - start with 'retrieve_' (the word retrieve is exclusive to this)
        - followed by series type e.g. stocks, FX, etc. One word only
        - optionally followed by 'data' or 'series', where applicable

   Ex. 'retrieve_stock_series', `retrieve_cpi_series`, `retrieve_fx_series`

   Avoid a general 'retrieve_series' method. We're just standarding properties

    Parameters
    ----------
    timezone : str
        The timezone used for returning datetime object. Strongly recommended
        to leave as 'UTC'

    Attributes
    ----------
    source_name
    valid_name
    access_key
    api_url
    access_type
    access_log
    timezone
    """

    def __init__(self, timezone='UTC'):
        self._source_name = 'Source Name'
        self._valid_name = 'SourceName'
        self._access_key = '<apikey>'
        self._access_type = 'REST'
        self._api_url = 'https://apiurl.com/'
        self._access_log = {
            'total_requests': 0,
            'last_request': None
        }
        # self._access_transactions = list()
        self._timezone = pytz.timezone(timezone)

    @property
    def source_name(self):
        """str: The pretty name of the data source"""

        return self._source_name

    @property
    def valid_name(self):
        """str: Alphanumeric form and underscore of `source_name`"""

        return self._valid_name

    @property
    def access_key(self):
        """str or None: The API key used to access the web API"""

        return self._access_key

    @property
    def access_type(self):
        """str: The type of the data source e.g. REST, python client, etc."""

        return self._access_type

    @property
    def api_url(self):
        """str or None: The API URL for accessing the resource"""

        return self._api_url

    @property
    def access_log(self):
        """dict: A running log of request operations"""

        return self._access_log

    @property
    def timezone(self):
        return self._timezone

    def _update_log(self):
        """Internal method to update the `access_log` property"""

        self._access_log['total_requests'] += 1
        self._access_log['last_request'] = self._timezone.localize(dt.utcnow())


class AlphaAdvantage(DataSource):
    """Access stock data from Alpha Vantage

    Alpha Vantage offers free stock data through a web API. Class currently
    only supports EOD stock prices

    Parameters
    ----------
    timezone : str
        The timezone used for returning datetime object. Strongly recommended
        to leave as 'UTC'

    Attributes
    ----------
    source_name
    valid_name
    access_key
    api_url
    access_type
    access_log
    series
    timezone
    """

    def __init__(self, timezone='UTC'):
        super().__init__(timezone)

        self._source_name = 'Alpha Advantage'
        self._valid_name = 'AlphaVantage'
        self._access_key = 'ARH5UW8CMDRTXLDM'
        self._api_url = 'https://www.alphavantage.co/query'
        self._access_type = 'REST'
        self._access_log = {
            'total_requests': 0,
            'last_request': None
        }

        self._series = {
            'ts_stock_d': 'TIME_SERIES_DAILY',
            'ts_stock_w': 'TIME_SERIES_WEEKLY',
            'ts_stock_m': 'TIME_SERIES_MONTHLY',
            'ts_stock_da': 'TIME_SERIES_DAILY_ADJUSTED',
            'ts_stock_wa': 'TIME_SERIES_WEEKLY_ADJUSTED',
            'ts_stock_ma': 'TIME_SERIES_MONTHLY_ADJUSTED'
        }

        self._dtype_map = {
            '1. open': np.float64,
            '2. high': np.float64,
            '3. low': np.float64,
            '4. close': np.float64,
            '5. volume': np.int64,
            '6. volume': np.int64,
            '5. adjusted close': np.float64,
            '7. dividend amount': np.float64,
            '8. split coefficient': np.float64
        }

        self._column_rename = {
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume',
            '6. volume': 'volume',
            '5. adjusted close': 'adjusted_close',
            '7. dividend amount': 'dividend_amount',
            '8. split coefficient': 'split_coefficient'
        }

        self._default_period = 'ts_stock_da'
        self._default_output = 'compact'
        self._timezone = pytz.timezone(timezone)

    @property
    def series(self):
        """dict: Contains available series and their function mapping"""
        return self._series

    def retrieve_stock_series(self, symbol, series='ts_stock_da',
                              output='compact'):
        """Retrieve time series data from Alpha Vantage

        Parameters
        ----------
        symbol : str
            The stock symbol for the time series. See property `functions`
        output : str
            Either 'compact' for the last 100 records, or 'full' for 20 years

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame`
            A `pandas.core.frame.DataFrame` that holds the time series data
            from Alpha Vantage

            - The data frame is always indexed on 'Symbol' and 'Datetime'
            - 'Datetime' is stored as a :obj:`datetime.datetime` that is
              localized to 'UTC' time
            - The columns returned are variable, depending on the time series
        """

        if series not in self._series.keys():
            errmsg = 'param:series must be one of'
            errmsg += ", ".join(self._series.keys())

            raise ValueError(errmsg)

        if output not in ['compact', 'full']:
            raise ValueError("param:output must be one of 'compact', 'full'")

        if isinstance(symbol, str) is not True:
            raise TypeError('param:symbol must be a string')

        resource = self._retrieve(series=series, symbol=symbol, output=output)

        histprice = pandas.DataFrame.from_dict(resource['series'])
        histprice['Datetime'] = pandas.DatetimeIndex(histprice['Datetime'],
                                                     tz=pytz.timezone('UTC'),
                                                     name="Datetime")
        result = (
            histprice.set_index(['Symbol', 'Datetime'], verify_integrity=True)
            .rename(index=str, columns=self._column_rename)
        )

        self._update_log()

        return result

    def _retrieve(self, series, symbol, output, datatype='json'):
        params = {
            'function': self._series[series],
            'symbol': symbol,
            'apikey': self._access_key,
            'outputsize': output,
            'datatype': datatype
        }

        req = requests.get(self._api_url, params=params)

        req_metadata = req.json()['Meta Data']

        # Get time zone
        data_tz = pytz.timezone(req_metadata['5. Time Zone'])

        # Extract the time series stock prices
        ts_key = [i for i in req.json().keys() if i != 'Meta Data'][0]

        req_result = req.json()[ts_key]

        for date, row in req_result.items():
            for row_element_name in row:
                val = row[row_element_name]
                row[row_element_name] = self._dtype_map[row_element_name](val)

            row['Datetime'] = _to_datetime_utc(date, '%Y-%m-%d')
            row['Symbol'] = symbol

        ts_data = [i for i in req_result.values()]

        result = {
            'meta_data': req_metadata,
            'timezone': data_tz,
            'series': ts_data
        }

        return result


class Barchart(DataSource):
    """Access stock data from barchart

    barchart offers free stock data through a web API. Class currently
    only supports EOD stock prices through the 'getQuote' and 'getHistory' API

    Parameters
    ----------
    timezone : str
        The timezone used for returning datetime object. Strongly recommended
        to leave as 'UTC'

    Attributes
    ----------
    source_name
    valid_name
    access_key
    api_url
    access_type
    access_log
    timezone
    """

    def __init__(self, timezone='UTC'):
        self._source_name = 'barchart'
        self._valid_name = 'barchart'
        self._access_key = 'edfaca2b49e5b010c2c39298a89a37ac'
        self._access_type = 'REST'
        self._api_url = 'https://marketdata.websol.barchart.com'
        self._access_log = {
            'total_requests': 0,
            'last_request': None
        }
        # self._access_transactions = list()
        self._timezone = pytz.timezone(timezone)

        self._dtype_map = {
            'symbol': str,
            'name': str,
            'exchange': str,
            'tradeTimestamp': _to_datetime_utc,
            'open': np.float64,
            'low': np.float64,
            'high': np.float64,
            'lastPrice': np.float64,
            'close': np.float64,
            'volume': np.int64,
            'percentChange': np.float64,
            'netChange': np.float64,
            'mode': str,
            'dayCode': str,
            'flag': str,
            'unitCode': str,
            'serverTimestamp': _to_datetime_utc,
            'tradeTimestamp': _to_datetime_utc
        }

        self._column_rename = {
            'symbol': 'Symbol',
            'tradeTimestamp': 'Datetime'
        }

    def retrieve_securities_quote(self, symbols):
        if isinstance(symbols, str):
            symbolList = [symbols]
        else:
            symbolList = symbols

            if len(symbolList) > 25:
                symbolList = symbolList[0:25]

        symbolList = ','.join(symbolList)

        resource = self._retrieve('getQuote.json', symbolList)

        quotes = (
            pandas.DataFrame.from_dict(resource)
            .rename(index=str, columns=self._column_rename)
            .set_index(['Symbol', 'Datetime'])
        )

        return quotes

    def _retrieve(self, endpoint, symbols):
        api_url = '/'.join([self._api_url, endpoint])

        params = {
            'apikey': self._access_key,
            'symbols': symbols
        }

        req = requests.get(api_url, params=params)

        symbol_quotes = req.json()['results']

        for a_quote in symbol_quotes:
            for colname in a_quote:
                a_quote[colname] = self._dtype_map[colname](a_quote[colname])

        return symbol_quotes


def _to_datetime_utc(datetime_str, dtformat='%Y-%m-%dT%H:%M:%S',
                     tz=pytz.timezone('US/Eastern')):
    r = dt.strptime(datetime_str[0:19], dtformat)
    r = tz.localize(r).astimezone(pytz.timezone('UTC'))

    return r
