from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, BigInteger
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime as dt


SQLBASE = declarative_base()


class StockDB:
    """Access the on-file, SQLite database

    This is the main financial and economic database, storing security prices,
    economic indicators, and table update logs

    Parameters
    ----------
    filename : str
        Filename, path included, of the SQLite database
    autocommit : bool
        Should the class auto commit edits to the database. This only happens
        when closing the session I believe

    Attributes
    ----------
    autocommit
    """

    def __init__(self, filename, autocommit=False):
        self._dbengine = create_engine('sqlite:///' + filename, echo=False)
        self._dbsession_factory = sessionmaker(bind=self._dbengine)
        self._dbsession = self._dbsession_factory(autocommit=autocommit)
        self._autocommit = autocommit

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, should_autocommit):
        if isinstance(should_autocommit, bool) is not True:
            raise TypeError('param: should_autocommit should be a boolean')

        self._autocommit = should_autocommit

    def commit(self):
        """Commit edits to database"""

        if self._autocommit is True:
            pass
        else:
            self._dbsession.commit()

    def list_tables(self):
        """List all tables declared

        Returns
        -------
        list of str
            List of all tables declared
        """

        return self._dbengine.table_names()

    def has_table(self, tablename):
        """Check if database has table

        Parameters
        ----------
        tablename : str
            Table name to check for

        Returns
        -------
        bool
            `True` if the table exists, otherwise `False`
        """

        return tablename in self.list_tables()

    def table_schema(self, tablename):
        """Return the table's schema

        The return value contains metadata for each column

        Parameters
        ----------
        tablename : str
            Table name to check for

        Returns
        -------
        list of dict
            Each `dict` represents one column

        Return Example
        --------------
        [
            {
                'autoincrement': 'auto',
                'default': None,
                'name': 'Sector',
                'nullable': True,
                'primary_key': 0,
                'type': TEXT()
            },
            {
                'autoincrement': 'auto',
                'default': None,
                'name': 'Industry',
                'nullable': True,
                'primary_key': 0,
                'type': TEXT()
            },
            ...
        ]
        """

        return inspect(self._dbengine).get_columns(tablename)

    def create_all_tables(self):
        """Create all tables declared"""
        SQLBASE.metadata.create_all(self._dbengine)

    def delete_table(self, tablename):
        """Delete a specific table

        Parameters
        ----------
        tablename : str
            Table name to check for
        """

        self.table_map(tablename).drop(bind=self._dbengine)

    def clear_records(self, tablename):
        """Clear all rows of a table

        Parameters
        ----------
        tablename : str
            Table name to check for
        """

        self._dbsession.query(self.table_map(tablename)).delete()

    def retrieve_prices(self, symbols=None, period='eod', startdate=None,
                        enddate=None):
        """Retrieve price data

        Currently only supports period='eod' for end of day prices

        Parameters
        ----------
        symbols : str, list of str
            Stock ticker to filter for. If `None`, all stock tickers are
            returned
        period: str
            Either 'eod' for end of day, or `intraday`, which is not supported
            and ignored
        startdate, enddate : str
            The start, end date to filter for (inclusive). Make sure `str` is
            in ISO format i.e. %Y-%m-%d

        Returns
        -------
        :obj:`pandas.core.frame.DataFrame`
            Indexed on 'Symbol' and 'Datetime', with open/close/high etc.

        """

        table_obj = None
        if period == 'eod':
            table_obj = self.table_map('eod_stockprices')

        elif period == 'intraday':
            pass

        else:
            errmsg = "param: tablename must be either 'eod', 'intraday'"
            raise ValueError(errmsg)

        query_obj = self._dbsession.query(table_obj)

        symbol_list = list()
        if isinstance(symbols, str):
            symbol_list.append(symbols)
        else:
            symbol_list = symbols

        if symbols is not None:
            sym_col = table_obj.columns['Symbol']
            query_obj = query_obj.filter(sym_col.in_(symbol_list))

        if startdate is not None:
            dt_col = table_obj.columns['Datetime']
            query_obj = query_obj.filter(dt_col >= startdate)

        if enddate is not None:
            dt_col = table_obj.columns['Datetime']
            query_obj = query_obj.filter(dt_col <= enddate)

        keys = self.table_keys('eod_stockprices')

        result = pd.read_sql(query_obj.statement, query_obj.session.bind,
                             index_col=keys)

        return result

    def insert_record(self, tablename, record):
        """Insert a new record

        If record is a DataFrame, then this is effectively bulk insert

        Parameters
        ----------
        tablename : str
        record: dict or pandas DataFrame
        """

        if isinstance(record, dict):
            table = self.table_map(tablename)
            self._dbsession.execute(table.insert(values=record))

        if isinstance(record, pd.core.frame.DataFrame):
            record.to_sql(tablename, self._dbengine, if_exists='append',
                          index=True)

        else:
            raise TypeError('param: record should be a dict, pandas')

    def bulk_insert_records(self, tablename, dataframe):
        if isinstance(dataframe, pd.core.frame.DataFrame):
            dataframe.to_sql(tablename, self._dbengine, if_exists='append',
                             index=True)

    def update_company(self, dataframe):
        dataframe.to_sql('company', self._dbengine, if_exists='replace',
                         index=False)

    def retrieve_company(self, columns=None, symbol=None):
        df_com = None

        keys = self.table_keys('company')

        if columns is None:
            df_com = pd.read_sql('company', self._dbengine, index_col=keys)
        else:
            if isinstance(columns, str):
                columns = [columns]

                if 'Symbol' not in columns:
                    columns.insert(0, 'Symbol')

            else:
                if 'Symbol' not in columns:
                    columns = ['Symbol'] + columns

            df_com = pd.read_sql('company', self._dbengine, columns=columns,
                                 index_col=keys)

        if symbol is not None:
            if isinstance(symbol, str):
                symbol = [symbol]

            df_com = df_com[df_com['Symbol'].isin(symbol)]

        return df_com

    def table_map(self, tablename):
        if self.has_table(tablename) is not True:
            errmsg = '\'{}\' table does not exist'.format(tablename)
            raise NoTableError(errmsg)

        return SQLBASE.metadata.tables[tablename]

    def table_keys(self, tablename):
        schema = self.table_schema(tablename)
        keys = [i['name'] for i in schema if i['primary_key'] != 0]
        keys = None if len(keys) == 0 else keys

        return keys


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class NoTableError(Error):
    """Exception for when a table requested does not exist

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class Company(SQLBASE):
    __tablename__ = 'company'
    Symbol = Column(String, primary_key=True)
    Name = Column(String)
    LastSale = Column(Integer)
    MarketCap = Column(Integer)
    ADR_TSO = Column(String)
    IPOyear = Column(Integer)
    Sector = Column(String)
    Industry = Column(String)
    Summary_Quote = Column(String)
    ExchangeListing = Column(String)
    Update_dt = Column(String)


class EODPrices(SQLBASE):
    __tablename__ = 'eod_stockprices'
    Symbol = Column(String, primary_key=True)
    Datetime = Column(DateTime, primary_key=True)
    open = Column(BigInteger)
    high = Column(BigInteger)
    low = Column(BigInteger)
    close = Column(BigInteger)
    volume = Column(BigInteger)


class UpdateLog(SQLBASE):
    __tablename__ = 'update_log'
    Datetime = Column(DateTime, primary_key=True)
    Source = Column(String, primary_key=True)
    Table = Column(String, primary_key=True)
    UpdateType = Column(String, primary_key=True)
    num_new_records = Column(Integer)
    num_deleted_records = Column(Integer)
    num_updated_records = Column(Integer)
    relevant_datetime = Column(DateTime)
