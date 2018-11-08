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
from requests.auth import HTTPBasicAuth
import tqdm
import backoff


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

    result = result[['Name', 'update_dt']].rename(columns={'Name': 'name'})

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
            histprice.set_index(['symbol', 'date'], verify_integrity=True)
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
            raise GeneralCallError("Unknown issue, with no solution")

        while True:
            if len(self._req_object.json().keys()) < 2:
                try:
                    req_key = list(self._req_object.json().keys())[0]

                except IndexError:
                    raise GeneralCallError('Index error was hit, unknown')

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

            row['date'] = _set_tz(date, '%Y-%m-%d', tz_f=self._timezone)
            row['date'] = row['date'].date()
            row['symbol'] = symbol

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


class Intrinio:
    def __init__(self, verbose=True):
        self._source_name = 'Intrinio'
        self._valid_name = 'intrinio'

        self._auth = HTTPBasicAuth('7a8c64cd06c90a7bed669378ecf218b7',
                                   '496f710575be1b715872116b4b716abd')

        self._api_url = 'https://api.intrinio.com'
        self._credits_used = 0

        self._apitz = pytz.timezone('US/Eastern')
        self._stz = pytz.timezone('UTC')

        self._verbose = verbose

        self._max_retries = 15

    @property
    def url(self):
        return self._api_url

    @property
    def used_credits(self):
        return self._credits_used

    @property
    def input_timezone(self):
        return self._apitz

    @property
    def output_timezone(self):
        return self._stz

    @property
    def is_verbose(self):
        return self._verbose

    @property
    def max_retries(self):
        return self._max_retries

    @property
    def source_name(self):
        return self._source_name

    @property
    def valid_name(self):
        return self._valid_name

    def get_securities_list(self, identifier=None, query=None,
                            exchange_symbol=None, us_only=True):
        """Get a listing of all securities within a stock exchange

        Method parameters are used to filter the listing. If both are `None`
        then the whole listing will be returned

        Parameters
        ----------
        identifier : str, default None
            The identifier for the security. If `None`, will be ignored
        query : str, default None
            Query search over the security name or ticker symbol. If `None`,
            will be ignored
        exchange_symbol : str, default None
            The Intrinio Stock Market Symbol, used to specify which exchange
            to look into. If `None`, all stock exchanges are considered
        us_only : bool, default True
            Retrieve only US-based securities

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame` or `None`
            The data frame will contain all listings of the stock exchange
            that meets the criteria. If `None`, then no results was received

            The `pandas.core.frame.DataFrame` it has the following structure:

            Index
            =====
            symbol : str
                Stock exchange symbol/ticker of the securities
            figi : str
                The OpenFIGI identifier of the securities
            exchange : str
                The stock exchange the securities are listed in

            Columns
            =======
            security_name : str
                Security description proved by the exchange
            security_type : str
                Category type of the securities e.g. common stock, preferred
            primary_security : bool
                Is security a primary issue
            currency : str
                The security's traded currnecy
            market_sector : str
                The type of market for the security
            figi_ticker : str
                The OpenFIGI ticker symbol

        Raises
        ------
            LimitError
                Requests have used up the alloted number of API calls
            ServerError
                A generic server error was recieved. Problem on the server-side
            ServiceUnavailableError
                Either throttle limit hit or high system load, hence the call
                was ignored
        """

        params = {'identifier': identifier, 'query': query,
                  'exch_symbol': exchange_symbol,
                  'us_only': 'Yes' if us_only else 'No',
                  'page_size': 100}

        api_url = self._api_url + "/securities"

        endpoint_getter = self.api_getter(api_url, params)
        data = self._execute_getter(endpoint_getter)

        data = data if data else None

        if data is not None:
            col_rename = {
                'ticker': 'symbol', 'stock_exchange': 'exchange',
                'figi': 'figi', 'composite_figi': 'cfigi',
                'figi_ticker': 'figi_ticker',
                'composite_figi_ticker': 'cfigi_ticker'
            }

            result = (
                pandas.DataFrame(data).
                rename(columns=col_rename).
                set_index(['symbol', 'figi', 'exchange'])
            )

            result = result[~result.index.duplicated(keep='first')]

            col_order = ['security_name', 'security_type', 'primary_security',
                         'currency', 'market_sector', 'figi_ticker',
                         'cfigi_ticker', 'delisted_security',
                         'last_crsp_adj_date']

            result = result[col_order]
        else:
            result = None

        return result

    def get_exchanges(self, identifier=None, query=None):
        """Get a listing of all Stock Exchanges covered by the source

        Method parameters are used to filter the listing. If both are `None`
        then the whole listing will be returned

        Parameters
        ----------
        identifier : str, default None
            The identifier for a stock exchange, which can be a MIC, symbol, or
            acronym. If `None`, the call will ignore the identifier
        query : str, default None
            Query search of the exchange's name or MIC. If `None`, the call
            will ignore the query

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame` or `None`
            The data frame will contain all listings of the stock exchange
            that meets the criteria. If `None` than no results was received

            The `pandas.core.frame.DataFrame` it has the following structure:

            Index
            =====
            symbol : str
                Stock exchange symbol/ticker of the security
            mic : str
                Identifier of the Stock Exchange

            Columns
            =======
            institution_name : str
                The full name of the exchange
            acronym : str
                Shorthand acronym of the exchange
            country : str
                Country where the exchange resides
            country_code : str
            city : str
                City where the exchange resides
            website : str

        Raises
        ------
            LimitError
                Requests have used up the alloted number of API calls
            ServerError
                A generic server error was recieved. Problem on the server-side
            ServiceUnavailableError
                Either throttle limit hit or high system load, hence the call
                was ignored
        """

        params = {'identifier': identifier, 'query': query, 'page_size': 100}

        api_url = self._api_url + "/stock_exchanges"

        endpoint_getter = self.api_getter(api_url, params)
        data = self._execute_getter(endpoint_getter)

        data = data if data else None

        if data is not None:
            col_rename = {'symbol': 'symbol', 'mic': 'mic'}

            result = (
                pandas.DataFrame(data).
                rename(columns=col_rename).
                set_index(['mic', 'symbol'])
            )

            col_order = ['institution_name', 'acronym', 'country',
                         'country_code', 'city', 'website']

            result = result[col_order]
        else:
            result = None

        return result

    def get_prices(self, identifier, start=None, end=None, freq='daily'):
        params = {'identifier': identifier, 'start_date': start,
                  'end_date': end, 'frequency': freq, 'page_size': 100}

        api_url = self._api_url + "/prices"

        endpoint_getter = self.api_getter(api_url, params)
        data = self._execute_getter(endpoint_getter)

        data = data if data else None

        if data is not None:
            col_tonumeric = ['open', 'low', 'high', 'close', 'adj_open',
                             'adj_low', 'adj_high', 'adj_close', 'adj_factor',
                             'split_ratio', 'ex_dividend', 'adj_volume']

            col_order = ['open', 'low', 'high', 'close', 'volume', 'adj_open',
                         'adj_low', 'adj_high', 'adj_close', 'adj_volume',
                         'adj_factor', 'split_ratio', 'ex_dividend']

            col_index = ['symbol', 'date']

            result = pandas.DataFrame(data)

            result['date'] = (
                result['date'].
                apply(lambda x: _set_tz(x, '%Y-%m-%d', self._apitz, self._stz,
                                        True).date())
            )
            # Does not make datetime naive
            # result['date'] = pandas.to_datetime(result['date'], utc=True)

            result[col_tonumeric] = (result[col_tonumeric].
                                     apply(pandas.to_numeric, errors='coerce'))

            result['symbol'] = identifier

            result = result.set_index(col_index)

            result['volume'] = result['volume'].astype(np.int64)

            result = result[col_order]

            result = result[~result.index.duplicated(keep='first')]
            result = result.sort_index(0)
        else:
            result = None

        return result

    def get_exchange_prices(self, identifier, date=None):
        """Get all stock prices in an exchange on a certain date

        Parameters
        ----------
        identifier : str
            The stock exchange's ticker symbol e.g. ARCX (NYSE), NASDAQ, AMEX
        date : str, default None
            String that has the date of interest. Should be in ISO format. If
            `None` then it is assumed be to the last full business day

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame` or `None`
            The data frame will contain all stocks from desired stock exchange,
            from the desired date. If `None` than no results was received

            The `pandas.core.frame.DataFrame` it has the following structure:

            Index
            =====
            symbol : str
                Stock exchange symbol/ticker of the security
            exchsym : str
                Identifier of the Stock Exchange
            date : `datetime.date`
                UTC date of the trade day

            Columns
            =======
            open, low, high, close : float
                Price metrics, in USD, of the security
            adj_open, adj_low, adj_high, adj_close : float
                Adjusted version of above price metrics
            volume, adj_volume : float
                Actual, and adjusted number of stocks traded amongst market
                participants on trading date
            adj_factor : float
            split_ratio : float
                The split factor of the split date
            ex_dividend : float
                The non-split adjusted dividend per share on the ex-dividend
                date

        Raises
        ------
            LimitError
                Requests have used up the alloted number of API calls
            ServerError
                A generic server error was recieved. Problem on the server-side
            ServiceUnavailableError
                Either throttle limit hit or high system load, hence the call
                was ignored
        """

        params = {'identifier': identifier, 'price_date': date,
                  'page_size': 100}

        api_url = self._api_url + "/prices/exchange"

        endpoint_getter = self.api_getter(api_url, params)
        data = self._execute_getter(endpoint_getter)

        data = data if data else None

        if data is not None:
            col_rename = {
                'ticker': 'symbol', 'figi': 'figi', 'composite_figi': 'cfigi',
                'figi_ticker': 'figi_ticker'
            }

            col_tonumeric = ['open', 'low', 'high', 'close', 'adj_open',
                             'adj_low', 'adj_high', 'adj_close', 'adj_factor',
                             'split_ratio', 'ex_dividend', 'adj_volume']

            col_order = ['open', 'low', 'high', 'close', 'volume', 'adj_open',
                         'adj_low', 'adj_high', 'adj_close', 'adj_volume',
                         'adj_factor', 'split_ratio', 'ex_dividend']

            col_index = ['symbol', 'exchsym', 'date']

            result = pandas.DataFrame(data).rename(columns=col_rename)

            result['date'] = (
                result['date'].
                apply(lambda x: _set_tz(x, '%Y-%m-%d', self._apitz, self._stz,
                                        True).date())
            )

            result[col_tonumeric] = (result[col_tonumeric].
                                     apply(pandas.to_numeric, errors='coerce'))

            result['exchsym'] = identifier

            result = result.set_index(col_index)

            result['volume'] = result['volume'].astype(np.int64)

            result = result[col_order]

            result = result[~result.index.duplicated(keep='first')]
            result = result.sort_index(0)
        else:
            result = None

        return result

    def _execute_getter(self, getter):
        if self._verbose is True:
            objiter = tqdm.tqdm(getter, ncols=79)

        else:
            objiter = getter

        payload = list(objiter)

        try:
            data = [i['data'] for i in payload]
            data = [item for sublist in data for item in sublist]
        except KeyError:
            data = payload

        try:
            self._credits_used += sum([i['api_call_credits'] for i in payload])
        except KeyError:
            self._credits_used += 1

        return data

    def api_getter(self, url, params):
        """Intrinio API request generator

        Automates the REST GET request, handling single and paging requests

        Parameters
        ----------
        url : str
            API url with the endpoint
        params : dict
            A dictionary keyed on the endpoint's parameters

        Yields
        ------
        dict
            Actual output will vary depending on the endpoint. For most calls
            it'll be the raw JSON payload, converted to a `dict` object

        Raises
        ------
            LimitError
                Requests have used up the alloted number of API calls
            ServerError
                A generic server error was recieved. Problem on the server-side
            ServiceUnavailableError
                Either throttle limit hit or high system load, hence the call
                was ignored
        """
        params['page_number'] = 1
        waitfun = backoff.jittered_backoff(32, verbose=False)
        session = requests.Session()

        while True:
            job_result = session.get(url, params=params, auth=self._auth)

            if job_result.ok is not True:
                if job_result.status_code == 401:
                    msg = " ".join(job_result.json()['errors'][0].values())

                    raise UnauthorizedCallError(msg)

                elif job_result.status_code == 403:
                    raise ForbiddenCallError(msg)

                elif job_result.status_code == 404:
                    endpoint = url.replace(self._api_url, '')

                    msg = f"Resource endpoint '{endpoint}' not found"

                    raise ResourceNotFoundError(msg)

                elif job_result.status_code in [429, 500, 503]:
                    waiting_result = waitfun()

                    if waiting_result['ntries'] >= self._max_retries:
                        if job_result.status_code == 429:
                            msg = job_result.json()['errors']['message']

                            raise LimitError(msg)

                        elif job_result.status_code == 500:
                            raise ServerError('Resource server error')

                        elif job_result.status_code == 503:
                            msg = 'Service unavailable'
                            raise ServiceUnavailableError(msg)

                else:
                    raise GeneralCallError("General error was thrown")

            job_result = job_result.json()

            yield job_result

            try:
                total_pages = job_result['total_pages']

            except KeyError:
                break

            if params['page_number'] < total_pages:
                params['page_number'] += 1

            else:
                break


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
        self.message = message


class UnauthorizedCallError(Error):
    """Exception raised when the call to the resource is Unauthorized

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class ForbiddenCallError(Error):
    """Exception raised when the call to the resource is forbiddened

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class InvalidCallError(Error):
    """Exception raised for when an API call has hit rate limits

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class ResourceNotFoundError(Error):
    """Exception raised for resource requested is not found

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class GeneralCallError(Error):
    """Exception raised for a general error, with undefined solution

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class ServerError(Error):
    """Exception raised for a general server error

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class ServiceUnavailableError(Error):
    """Exception raised for unavailable resource

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class RateLimitError(Error):
    """Exception raised for a general error, with undefined solution

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class LimitError(Error):
    """Exception raised for a resource has reached a limit

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
