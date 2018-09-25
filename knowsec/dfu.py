import requests
import re
import json
import datetime as dt
import pandas
import pytz
from bs4 import BeautifulSoup
from abc import ABCMeta, abstractmethod
import numpy as np
import functools
import decimal


STOCKSERIES_COLUMNS = ['open', 'close', 'high', 'low', 'volume']

PDIDX = pandas.IndexSlice

FIFACTOR = 10000

LISTING = ['NYSE', 'AMEX', 'NASDAQ', 'ETF']


def download_symbols(exchange):
    """Security symbols sourced from NASDAQ

    Downloads, parses, and returns a Pandas DataFrame containing stock symbols
    for the NASDAQ, NYSE, and AMEX exchanges, as well as ETFs

    Parameters
    ----------
        exchange : str
            The exchange name. Can be one of 'NASDAQ', 'NYSE', 'AMEX', or 'ETF'

    Returns
    -------
    :obj:`pandas.core.frame.DataFrame`
        The data frame object is multi-indexed on 'Symbol' and 'Listing', and
        has only one column: 'Name'
    """

    if exchange not in LISTING:
        allowed_ex = ["'" + i + "'" for i in LISTING]
        allowed_ex = ", ".join(allowed_ex)
        raise ValueError(f'param: exchange must be one of {allowed_ex}')

    if exchange == 'ETF':
        url = "https://www.nasdaq.com/investing/etfs/etf-finder-results.aspx"
        url += "?download=yes"
    else:
        url = "https://www.nasdaq.com/screening/companies-by-industry.aspx"
        url += f"?render=download&exchange={exchange}"

    result = pandas.read_csv(url, delimiter=",", header=0, encoding='ascii')
    result['Symbol'] = result['Symbol'].apply(lambda x: x.strip())
    result['Name'] = result['Name'].apply(lambda x: x.strip())
    result['Name'] = result['Name'].apply(lambda x: x.replace('&#39;', '’'))
    result['update_dt'] = dt.datetime.utcnow()

    result.insert(1, 'Listing', exchange)
    result = result.set_index(['Symbol', 'Listing'])

    result = result[['Name', 'update_dt']]

    return result


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
    payload = json.dumps({"seriesid": series, "startyear": startyear,
                          "endyear": endyear})

    req = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                        data=payload,
                        headers=headers)

    json_data = json.loads(req.text)

    result = list()
    for series in json_data['Results']['series']:
        series_id = series['seriesID']

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
            dfr['SeriesID'] = series_id

            result.append(dfr)

    for i in range(1, len(result)):
        result[0] = result[0].append(result[i], ignore_index=True)

    if 'latest' in result[0].columns:
        result[0] = result[0].drop('latest', axis=1)

    return result[0]


class DataSource(metaclass=ABCMeta):
    """Parent class defining a standardized data source API

    Classes that inherit this metaclass will have standardized properties and
    methods useful to retrieve data and get information about the data source
    itself, such as the name, api keys (if applicable), request logs, etc.

    The primary way to interact with classes that inherit this metaclass is
    the abstract method `get_data`, with parameters as needed. This method
    should return `pandas.core.frame.DataFrame` whenever possible. Other
    structures are allowed where when needed however. The output data
    structure, types, and others must be explicitly described in the method's
    docstring

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
    access_log
    timezone
    req_object
    """

    def __init__(self, timezone='UTC'):
        self._source_name = 'Source Name'
        self._valid_name = 'SourceName'
        self._access_key = '<apikey>'
        self._api_url = 'https://apiurl.com/'
        self._access_log = {
            'total_requests': 0,
            'last_request': None
        }
        self._timezone = pytz.timezone(timezone)
        self._req_object = None

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

    @property
    def req_object(self):
        return self._req_object

    @abstractmethod
    def retrieve_data(self, symbol, series):
        pass

    @abstractmethod
    def _retrieve(self):
        pass

    @abstractmethod
    def retrieve_latest(self, from_dt, symbol, series):
        pass

    def _update_log(self):
        """Internal method to update the `access_log` property"""

        self._access_log['total_requests'] += 1
        self._access_log['last_request'] = (
            self._timezone.localize(dt.datetime.utcnow())
        )


class AlphaVantage(DataSource):
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
    access_log
    series
    timezone
    """

    def __init__(self, timezone='UTC'):
        super().__init__(timezone)

        self._source_name = 'Alpha Vantage'
        self._valid_name = 'AlphaVantage'
        self._access_key = 'ARH5UW8CMDRTXLDM'
        self._api_url = 'https://www.alphavantage.co/query'
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
            '1. open': (np.int64, decimal.Decimal, FIFACTOR),
            '2. high': (np.int64, decimal.Decimal, FIFACTOR),
            '3. low': (np.int64, decimal.Decimal, FIFACTOR),
            '4. close': (np.int64, decimal.Decimal, FIFACTOR),
            '5. volume': (np.int64, None, None),
            '6. volume': (np.int64, None, None),
            '5. adjusted close': (np.int64, decimal.Decimal, FIFACTOR),
            '7. dividend amount': (np.int64, decimal.Decimal, FIFACTOR),
            '8. split coefficient': (np.int64, decimal.Decimal, FIFACTOR)
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

    def retrieve_data(self, symbol, series='ts_stock_da', output='compact'):
        """Retrieve time series data from Alpha Vantage

        Parameters
        ----------
        symbol : str
            The stock symbol for the time series. See property `functions`
        series : str
            The type and period series to retrieve
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

        result = (
            histprice.set_index(['Symbol', 'Datetime'], verify_integrity=True)
            .rename(columns=self._column_rename)
            .sort_index()
        )

        return result

    def retrieve_latest(self, symbol, from_dt, series='ts_stock_da'):
        """Retrieve the latest time series data from Alpha Vantage

        Parameters
        ----------
        symbol : str
            Ticker symbol
        from_dt : datetime.datetime
            The datetime to filter from. Inclusive
        series : str
            The type and period series to retrieve

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame`
            A `pandas.core.frame.DataFrame` that holds the time series data
            from Alpha Vantage

            - The data frame is always indexed on 'Symbol' and 'Datetime'
            - 'Datetime' is stored as a :obj:`datetime.datetime` that is
              localized to 'UTC' time
            - The columns returned are variable, depending on the time series
            - Can be an empty data frame if no records were found
        """
        if isinstance(from_dt, dt.datetime) is True:
            initial_dt = from_dt.isoformat()
        else:
            errmsg = 'param: from_dt should be a {} object'
            errmsg = errmsg.format('datetime.datetime')
            raise TypeError(errmsg)

        latest_dt = dt.datetime.utcnow().isoformat()

        data = self.retrieve_data(symbol=symbol, series=series,
                                  output='compact')

        return data.loc[PDIDX[:, initial_dt:latest_dt], :]

    def _retrieve(self, series, symbol, output, datatype='json'):
        params = {
            'function': self._series[series],
            'symbol': symbol,
            'apikey': self._access_key,
            'outputsize': output,
            'datatype': datatype
        }

        self._req_object = requests.get(self._api_url, params=params)

        if self._req_object.ok is not True:
            raise self._req_object.raise_for_status()

        while True:
            if len(self._req_object.json().keys()) < 2:
                req_key = list(self._req_object.json().keys())[0]

                if req_key == 'Error Message':
                    # TODO log the message. Might be invalid API call
                    msg = self._req_object.json()[req_key]

                    if msg.lower().find('invalid api call') != -1:
                        raise InvalidCallError(msg)

                elif req_key == 'Information':
                    msg = self._req_object.json()[req_key]

                    if msg.lower().find('higher api call') != -1:
                        raise RateLimitError('Rate limit exceeded')

                else:
                    raise GeneralCallError("Unknown issue, with no solution")

            else:
                req_metadata = self._req_object.json()['Meta Data']

                break

        ts_key = [i for i in req_metadata.keys()
                  if re.search('time zone', i, re.I) is not None]

        # Get time zone
        if ts_key:
            data_tz = pytz.timezone(req_metadata[ts_key[0]])
        else:
            data_tz = pytz.timezone('US/Eastern')

        # Extract the time series stock prices
        ts_key = [i for i in self._req_object.json().keys()
                  if i != 'Meta Data'][0]

        req_result = self._req_object.json()[ts_key]

        for date, row in req_result.items():
            for row_element_name in row:
                val = row[row_element_name]
                dtype_map = self._dtype_map[row_element_name]

                if dtype_map[1] is None:
                    row[row_element_name] = dtype_map[0](val)

                else:
                    val = dtype_map[0](dtype_map[1](val) * dtype_map[2])
                    row[row_element_name] = val

            row['Datetime'] = _set_tz(date, '%Y-%m-%d', tz_f=self._timezone)
            row['Symbol'] = symbol

        ts_data = [i for i in req_result.values()]

        self._update_log()

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
        super().__init__(timezone)

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

        fmt_set_tz = functools.partial(_set_tz, dt_format='%Y-%m-%dT%H:%M:%S')

        self._dtype_map = {
            'symbol': str,
            'name': str,
            'exchange': str,
            'tradeTimestamp': fmt_set_tz,
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
            'serverTimestamp': fmt_set_tz,
            'tradeTimestamp': fmt_set_tz
        }

        self._column_rename = {
            'symbol': 'Symbol',
            'tradeTimestamp': 'Datetime'
        }

    def retrieve_data(self, symbol, mode=None, series=None):
        """Retrieve time series data from Barchart.com

        Parameters
        ----------
        symbol : str or list of str
            The stock symbol(s) for the time series
        mode : str or None
            Filters quote for recency. 'r' for real-time, 'i' for delayed, or
            'd' for end of day prices
        series : str, not yet implemented

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame`
            A `pandas.core.frame.DataFrame` that holds the time series data
            from Barchart.com

            - The data frame is always indexed on 'Symbol' and 'Datetime'
            - 'Datetime' is stored as a :obj:`datetime.datetime` that is
              localized to 'UTC' time
            - The columns returned are variable, depending on the time series
        """

        if isinstance(symbol, str):
            symbol_list = [symbol]
        else:
            if len(symbol) > 25:
                raise TooManySymbolsError('Max number of symbols is 25')

            symbol_list = symbol

        resource = self._retrieve('getQuote.json', ','.join(symbol_list))

        quotes = (
            pandas.DataFrame.from_dict(resource)
            .rename(columns=self._column_rename)
            .set_index(['Symbol', 'Datetime'])
        )

        if mode is not None:
            quotes = quotes[quotes['mode'] == mode]

        return quotes

    def retrieve_latest(self, symbol, from_dt=None, mode=None, series=None):
        """Retrieve the latest time series data from Alpha Vantage

        Parameters
        ----------
        symbol : str
            Ticker symbol
        from_dt : datetime.datetime, not yet implemented
        mode : str or None
            Filters quote for recency. 'r' for real-time, 'i' for delayed, or
            'd' for end of day prices
        series : str, not yet implemented

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame`
            A `pandas.core.frame.DataFrame` that holds the time series data
            from Alpha Vantage

            - The data frame is always indexed on 'Symbol' and 'Datetime'
            - 'Datetime' is stored as a :obj:`datetime.datetime` that is
              localized to 'UTC' time
            - The columns returned are variable, depending on the time series
            - Can be an empty data frame if no records were found
        """
        return self.retrieve_data(symbol=symbol, mode=mode)

    def _retrieve(self, endpoint, symbols):
        api_url = '/'.join([self._api_url, endpoint])

        params = {
            'apikey': self._access_key,
            'symbols': symbols
        }

        self._req_object = requests.get(api_url, params=params)

        symbol_quotes = self._req_object.json()['results']

        for a_quote in symbol_quotes:
            for colname in a_quote:
                a_quote[colname] = self._dtype_map[colname](a_quote[colname])

        self._update_log()

        return symbol_quotes


def _set_tz(dt_str, dt_format, tz_i=pytz.timezone('US/Eastern'),
            tz_f=pytz.timezone('UTC'), make_naive=True):
    """Convert a datetime string into native Python Datetime object

    The datetime string does not have to have time in it. However, the
    :obj:`datetime.datetime` returned will have time. It is assumed in this
    case that the time is 00:00:00 in the native timezone, defined by the
    `tz_i` parameter

    Previously, when attempting to save the datetime objects, I'd get a:
        ValueError: Cannot cast DatetimeIndex to dtype datetime64[us]

    Ensuring the datetime object is naive fixes this


    Parameters
    ----------
    dt_str : str
        The datetime string to convert
    dt_format : str
        The format the datetime string takes. Same as `datetime.datetime`
    tz_i : pytz.timezone
        States what timezone the datetime string is referring to
    tz_f : pytz.timezone
        States the output `datetime.datetime` object should be in
    make_naive : bool
        Should timezone information be stripped from the output
        `datetime.datetime` object

    Returns
    -------
    :obj:`datetime.datetime`
        A datetime object set in the timezone as specified in `dt_f`
    """
    new_datetime = dt.datetime.strptime(dt_str[0:19], dt_format)
    new_datetime = tz_i.localize(new_datetime).astimezone(tz_f)

    if make_naive is True:
        new_datetime = new_datetime.replace(tzinfo=None)

    return new_datetime


def standardize_stock_series(dataframe):
    return dataframe[STOCKSERIES_COLUMNS]


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class TooManySymbolsError(Error):
    """Exception raised for when too many symbols are requested

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        super().__init__()
        self.message = message


class InvalidCallError(Error):
    """Exception raised for when an API call has hit rate limits

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        super().__init__()
        self.message = message


class GeneralCallError(Error):
    """Exception raised for a general error, with undefined solution

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        super().__init__()
        self.message = message


class RateLimitError(Error):
    """Exception raised for a general error, with undefined solution

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        super().__init__()
        self.message = message
