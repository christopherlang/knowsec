from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, DateTime, BigInteger
import sqlalchemy_utils as sql_utils
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

    def rollback(self):
        self._dbsession.rollback()

    def delete_table(self, tablename):
        """Delete a specific table

        Parameters
        ----------
        tablename : str
            Table name to check for
        """

        self.table_map(tablename).drop(bind=self._dbengine)

    def clear_table(self, tablename):
        """Clear all rows of a table

        Parameters
        ----------
        tablename : str
        """

        self._dbsession.query(self.table_class(tablename)).delete()

    def insert_record(self, tablename, record):
        """Insert a new record

        If record is a DataFrame, then this is effectively bulk insert

        Parameters
        ----------
        tablename : str
        record: dict or pandas DataFrame

        Raises:
            IntegrityError
                The new record has keys that already exists in 'tablename'
            TypeError
                The new record is not of type `dict`,
                `pandas.core.frame.DateFrame`
        """

        if isinstance(record, dict):
            table_class = self.table_class(tablename)
            row = table_class(**record)

            self._dbsession.add(row)

        elif isinstance(record, pd.core.frame.DataFrame):
            record.to_sql(tablename, self._dbengine, if_exists='append',
                          index=True)

        else:
            raise TypeError('param: record must be a dict, DataFrame')

    def bulk_insert_records(self, tablename, records):
        """Bulk insert multiple records


        Parameters
        ----------
        tablename : str
        record: list of dict, or pandas DataFrame

        Raises:
            IntegrityError
                The new records has keys that already exists in 'tablename'
            TypeError
                The new records is not of type `list`,
                `pandas.core.frame.DateFrame`
        """

        if isinstance(records, list):
            table_class = self.table_class(tablename)
            self._dbsession.bulk_insert_mappings(table_class, records)

        elif isinstance(records, pd.core.frame.DataFrame):
            records.to_sql(tablename, self._dbengine, if_exists='append',
                           index=True)

    def update_record(self, tablename, record):
        table_class = self.table_class(tablename)
        table_cols = sql_utils.get_columns(self.table_class('exchanges'))
        table_keys = self.table_keys(tablename)

        if all([i in record.keys() for i in table_keys]) is not True:
            raise KeyError('param: record does not contain all primary keys')

        record_insert = record.copy()
        filter_clause = list()
        for pkey in table_keys:
            filter_clause.append(table_cols[pkey] == record[pkey])
            del record_insert[pkey]

        query_obj = self._dbsession.query(table_class).filter(*filter_clause)

        query_obj.update(record_insert)

    def update_records(self, tablename, records):
        for record in records:
            self.update_record(tablename, record)

    def replace_table(self, tablename, records):
        self.clear_table(tablename)
        self.bulk_insert_records(records)

    def slice_table(self, tablename, columns=None, filters=None,
                    index_keys=True):
        table_class = self.table_class(tablename)
        table_cols = sql_utils.get_columns(self.table_class('exchanges'))

        query = self._dbsession.query(table_class)

        if filters is not None:
            criteria = list()

            for col_name in filters:
                if isinstance(filters[col_name], (set, list, tuple)):
                    elements = filters[col_name]
                    criteria.append(table_cols[col_name].in_(elements))
                else:
                    criteria.append(table_cols[col_name] == filters[col_name])

            query = query.filter(*criteria)

        if columns is not None:
            select_columns = list()

            if isinstance(columns, (set, tuple, list)):
                for a_col in columns:
                    select_columns.append(table_cols[a_col])

            elif isinstance(columns, str):
                select_columns.append(table_cols[columns])

            query = query.with_entities(*select_columns)

        result_pd = pd.read_sql(query.statement, query.session.bind)

        if index_keys is True:
            pkeys = self.table_keys(tablename)
            pkeys = [i for i in pkeys if i in result_pd.columns]

            if pkeys:
                result_pd = result_pd.set_index(pkeys)

        return result_pd

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

    def table_columns(self, tablename):
        schema = self.table_schema(tablename)
        col_names = [i['name'] for i in schema]
        col_names = None if len(col_names) == 0 else col_names

        return col_names

    def table_class(self, tablename):
        table = self.table_map(tablename)
        table_class = sql_utils.get_class_by_table(SQLBASE, table)

        return table_class


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


class Securities(SQLBASE):
    __tablename__ = 'securities'
    Symbol = Column(String, primary_key=True)
    FIGI = Column(String, primary_key=True)
    Exchange = Column(String, primary_key=True)
    security_name = Column(String)
    security_type = Column(String)
    primary_security = Column(String)
    currency = Column(String)
    market_sector = Column(String)
    FIGI_ticker = Column(String)


class Exchanges(SQLBASE):
    __tablename__ = 'exchanges'
    Symbol = Column(String, primary_key=True)
    MIC = Column(String, primary_key=True)
    institution_name = Column(String)
    acronym = Column(String)
    country = Column(String)
    country_code = Column(String)
    city = Column(String)
    website = Column(String)


class Prices(SQLBASE):
    __tablename__ = 'security_prices'
    Symbol = Column(String, primary_key=True)
    Exchange = Column(String, primary_key=True)
    Datetime = Column(DateTime, primary_key=True)
    open = Column(BigInteger)
    high = Column(BigInteger)
    low = Column(BigInteger)
    close = Column(BigInteger)
    volume = Column(BigInteger)


class EODPrices_log(SQLBASE):
    __tablename__ = 'eod_stockprices_update_log'
    Symbol = Column(String, primary_key=True)
    minimum_datetime = Column(DateTime)
    maximum_datetime = Column(DateTime)
    update_dt = Column(DateTime)


# class UpdateLog(SQLBASE):
#     __tablename__ = 'update_log'
#     Datetime = Column(DateTime, primary_key=True)
#     Source = Column(String, primary_key=True)
#     Table = Column(String, primary_key=True)
#     UpdateType = Column(String, primary_key=True)
#     num_new_records = Column(Integer)
#     num_deleted_records = Column(Integer)
#     num_updated_records = Column(Integer)
#     relevant_datetime = Column(DateTime)
