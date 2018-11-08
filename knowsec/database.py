from sqlalchemy import create_engine, inspect, or_, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, DateTime, Date, Integer, BigInteger, Numeric, Boolean
import sqlalchemy_utils as sql_utils
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime as dt
import itertools
import collections


SQLBASE = declarative_base()


def constr_sqlite(filename):
    constr = f'sqlite:///{filename}'

    return constr


def constr_postgres(uid, pwd, host, port, dbname, kwargs=None):
    # postgresql+psycopg2://user:password@host:port/dbname
    constr = f'postgresql+psycopg2://{uid}:{pwd}@{host}:{port}/{dbname}'

    return constr


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

    def __init__(self, connection_string, echo=False):
        self._dbengine = create_engine(connection_string, echo=echo)
        self._dbsession_factory = sessionmaker(bind=self._dbengine)
        self._dbsession = self._dbsession_factory(autocommit=False)

    @property
    def session(self):
        return self._dbsession

    @property
    def engine(self):
        return self._dbengine

    def commit(self):
        self._dbsession.commit()

    def close(self):
        self._dbsession.close()

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

    def table_schema(self, tablename, as_dataframe=False):
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
                'primary_key': True,
                'type': TEXT()
            },
            {
                'autoincrement': 'auto',
                'default': None,
                'name': 'Industry',
                'nullable': True,
                'primary_key': True,
                'type': TEXT()
            },
            ...
        ]
        """

        keys = inspect(self.table_class(tablename)).primary_key
        keys = [i.name for i in keys]

        schema = inspect(self._dbengine).get_columns(tablename)
        for a_schema in schema:
            if a_schema['name'] in keys:
                a_schema['primary_key'] = True
            else:
                a_schema['primary_key'] = False

        if as_dataframe is True:
            schema = pd.DataFrame(schema)
            schema = schema.rename(columns={'name': 'Column_Name'})
            schema = schema[['Column_Name', 'primary_key', 'default',
                             'autoincrement', 'nullable', 'type']]

        return schema

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

    def retrieve_record(self, tablename, pkeys):
        """Retrieve record(s) by primary key

        The parameter `pkeys` is used to specify the filtering criteria by
        primary key. Hence `pkeys` should be keyed on the primary key

        Filtering specification follows method `slice_table`

        A primary key must be present in `pkeys`. If the number of primary
        keys supplied is less than the number of actual primary keys on the
        table, more than one record can potentially be returned

        Parameters
        ----------
        tablename : str
        pdkey: list of dict
            A `dict` keyed on primary key names, where values are used as
            filter criteria

        Returns
        -------
        :obj:`pandas.core.frame.DateFrame` or `None`
            The query result is returned as a pandas DataFrame. All columns
            in the table for that record will be returned and indexed on
            primary keys

            A `None` is returned if the results has zero rows
        """

        table_keys = self.table_keys(tablename)

        if any([i in pkeys.keys() for i in table_keys]) is not True:
            raise KeyError('param: record does not contain any primary keys')

        return self.slice_table(tablename, filters=pkeys, index_keys=True)

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
        """Update record in a table by key

        A `record` must have all primary keys present, but does not have to
        have all columns, just the ones that need updating

        Parameters
        ----------
        tablename : str
        record: dict
        """

        table_class = self.table_class(tablename)
        table_cols = sql_utils.get_columns(table_class)
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
        """Update multiple records in a table by key

        A `record` must have all primary keys present, but does not have to
        have all columns, just the ones that need updating

        This is the same as method `update_record` but loops through `records`

        Parameters
        ----------
        tablename : str
        record: list of dict
        """

        for record in records:
            self.update_record(tablename, record)

    def replace_table(self, tablename, records):
        """Clear records of a table and insert new records

        Parameters
        ----------
        tablename : str
        record: list of dict, or pandas DataFrame
        """

        self.clear_table(tablename)
        self.bulk_insert_records(records)

    def _raise_missing_pkeys(self, tablename, pkeys):
        if isinstance(pkeys, (list, set, tuple)):
            pkeys_provided = [i.keys() for i in pkeys]
        elif isinstance(pkeys, dict):
            pkeys_provided = [pkeys.keys()]

        table_keys = self.table_keys(tablename)
        has_pkeys = list()
        for a_pkey_provided in pkeys_provided:
            has_pkeys.append(all([i in a_pkey_provided for i in table_keys]))

        if all(has_pkeys) is not True:
            raise KeyError(f'Not all records have all primary keys')

        else:
            return True

    def record_exists(self, tablename, records, invert=False):
        self._raise_missing_pkeys(tablename, records)

        pkeys = self.table_keys(tablename)
        pkeys_only = list()
        table_filters = list()

        for rec in records:
            pkey_rec = {k: v for k, v in rec.items() if k in pkeys}
            pkeys_only.append(pkey_rec)
            table_filters.append(pkey_rec)

        result = list()
        for a_filter in table_filters:
            sub_query = self._construct_filter(a_filter, tablename)
            sub_query = sub_query.exists()
            sub_query = self._dbsession.query(sub_query)
            result.append(sub_query.scalar())

        if invert is True:
            result = [not i for i in result]

        return result

    def _construct_filter(self, filters, tablename, query=None):
        table_class = self.table_class(tablename)
        table_cols = sql_utils.get_columns(table_class)

        if query is None:
            query = self._dbsession.query(table_class)

        result_query = None

        if isinstance(filters, dict):
            criteria = list()

            for col_name in filters:
                if isinstance(filters[col_name], (set, list, tuple)):
                    elements = filters[col_name]
                    criteria.append(table_cols[col_name].in_(elements))
                else:
                    criteria.append(table_cols[col_name] == filters[col_name])

            result_query = query.filter(*criteria)

        else:
            or_criterias = list()

            for a_filter in filters:
                criteria = list()

                for col_name in a_filter:
                    if isinstance(a_filter[col_name], (set, list, tuple)):
                        elements = a_filter[col_name]
                        criteria.append(table_cols[col_name].in_(elements))
                    else:
                        criteria.append(table_cols[col_name] == a_filter[col_name])

                or_criterias.append(and_(*criteria))

            result_query = query.filter(or_(*or_criterias))

        return result_query

    def slice_table(self, tablename, columns=None, filters=None,
                    index_keys=True):
        """Retrieve data from table with basic slicing

        Will return data from a table on the database, and allows column
        selection, and basic filtering

        Filters should be a `dict` object, keyed on the columns to filter on.
        If more than one keys are present, it is assumed to be a logical AND:

        filters = {'Symbol': 'AMD', 'MIC': 'AMD'} becomes, in SQL
        WHERE Symbol = 'AMD' and MIC = 'AMD'

        Values can be a single values, such as strings or integers, in which
        case it is treated as a logical equality, or a collection, in which
        case it is treated as a IN statement:

        filters = {'Symbol': ['AMD', 'AAPL']} becomes, in SQL
        WHERE Symbol IN ('AMD', 'AAPL')

        Parameters
        ----------
        tablename : str
            The name of the table
        columns : str, list of str, None
            The columns to be returned. If `None`, then all columns
        filters : dict, list of dict, None
            A `dict` keyed on column names, where values are used as filter
            criteria. If `None`, then no filtering is performed
        index_keys : bool
            If columns in the returned query are primary keys, should the
            returned pandas DataFrame be indexed on them

        Returns
        -------
        :obj:`pandas.core.frame.DateFrame` or `None`
            The query result is returned as a pandas DataFrame. If `index_keys`
            is `True`, then indexed on primary key columns if those columns
            are returned

            A `None` is returned if the results has zero rows
        """

        table_class = self.table_class(tablename)
        table_cols = sql_utils.get_columns(table_class)

        query = self._dbsession.query(table_class)

        if filters is not None:
            if isinstance(filters, dict):
                criteria = list()

                for col_name in filters:
                    if isinstance(filters[col_name], (set, list, tuple)):
                        elements = filters[col_name]
                        criteria.append(table_cols[col_name].in_(elements))
                    else:
                        criteria.append(table_cols[col_name] == filters[col_name])

                query = query.filter(*criteria)

            else:
                or_criterias = list()

                for a_filter in filters:
                    criteria = list()

                    for col_name in a_filter:
                        if isinstance(a_filter[col_name], (set, list, tuple)):
                            elements = a_filter[col_name]
                            criteria.append(table_cols[col_name].in_(elements))
                        else:
                            criteria.append(table_cols[col_name] == a_filter[col_name])

                    or_criterias.append(and_(*criteria))

                query = query.filter(or_(*or_criterias))

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

        if result_pd.empty is True:
            result_pd = None

        return result_pd

    def table_map(self, tablename):
        """Convert a string table name to Table object

        Returns
        -------
        :obj:`sqlalchemy.sql.schema.Table`

        Raises
        ------
        NoTableError
            Table was not found
        """

        if self.has_table(tablename) is not True:
            errmsg = '\'{}\' table does not exist'.format(tablename)
            raise NoTableError(errmsg)

        return SQLBASE.metadata.tables[tablename]

    def table_keys(self, tablename):
        """Get table's primary keys

        Returns
        -------
        list of str
            A list of strings, naming the primary keys in the table
        """

        schema = self.table_schema(tablename)
        keys = [i['name'] for i in schema if i['primary_key'] is True]
        keys = keys if keys else None

        return keys

    def table_columns(self, tablename):
        """Get a list of column names of a table

        Returns
        -------
        list of str
            A list of strings, naming all columns in the table
        """

        schema = self.table_schema(tablename)
        col_names = [i['name'] for i in schema]
        col_names = None if len(col_names) == 0 else col_names

        return col_names

    def table_class(self, tablename):
        """Convert a string table name to Declartive Meta

        Primarily used to get the mapping class using SQLAlchemy ORM

        Returns
        -------
        :obj:`sqlalchemy.ext.declarative.api.DeclarativeMeta`
            Used in a lot of ORM querying, filtering, etc.
        """

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
    symbol = Column(String, primary_key=True)
    figi = Column(String, primary_key=True)
    exchange = Column(String, primary_key=True)
    security_name = Column(String)
    security_type = Column(String)
    primary_security = Column(String)
    currency = Column(String)
    market_sector = Column(String)
    figi_ticker = Column(String)
    cfigi_ticker = Column(String)
    delisted_security = Column(Boolean)
    last_crsp_adj_date = Column(Date)

    def __repr__(self):
        cols = [
            f"symbol='{self.Symbol}'",
            f"figi='{self.FIGI}'",
            f"exchange='{self.Exchange}'",
            f"security_name='{self.security_name}'",
            f"security_type='{self.security_type}'",
            f"primary_security='{self.primary_security}'",
            f"currency='{self.currency}'",
            f"market_sector='{self.market_sector}'"
            f"figi_ticker='{self.FIGI_ticker}'"
        ]

        repr_statement = ", ".join(cols)

        return "<Securities({})>".format(repr_statement)


class Exchanges(SQLBASE):
    __tablename__ = 'exchanges'
    symbol = Column(String, primary_key=True)
    mic = Column(String, primary_key=True)
    institution_name = Column(String)
    acronym = Column(String)
    country = Column(String)
    country_code = Column(String)
    city = Column(String)
    website = Column(String)

    def __repr__(self):
        cols = [
            f"symbol='{self.Symbol}'",
            f"mic='{self.MIC}'",
            f"institution_name='{self.institution_name}'",
            f"acronym='{self.acronym}'",
            f"country='{self.country}'",
            f"country_code='{self.country_code}'",
            f"city='{self.city}'",
            f"website='{self.website}'"
        ]

        repr_statement = ", ".join(cols)

        return "<Exchanges({})>".format(repr_statement)


class Prices(SQLBASE):
    __tablename__ = 'security_prices'
    symbol = Column(String, primary_key=True)
    exchange = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(BigInteger)
    adjusted_close = Column(Numeric)
    dividend_amount = Column(Numeric)
    split_coefficient = Column(Numeric)

    def __repr__(self):
        cols = [
            f"symbol='{self.Symbol}'",
            f"exchange='{self.Exchange}'",
            f"date='{self.Date}'",
            f"open='{self.open}'",
            f"high='{self.high}'",
            f"low='{self.low}'",
            f"close='{self.close}'",
            f"volume='{self.volume}'"
        ]

        repr_statement = ", ".join(cols)

        return "<Prices({})>".format(repr_statement)


class EODPrices_log(SQLBASE):
    __tablename__ = 'prices_log'
    symbol = Column(String, primary_key=True)
    min_date = Column(Date)
    max_date = Column(Date)
    update_dt = Column(DateTime(timezone=True))
    check_dt = Column(DateTime(timezone=True))


class Update_log(SQLBASE):
    __tablename__ = 'update_log'
    table = Column(String, primary_key=True)
    update_dt = Column(DateTime, primary_key=True)
    update_type = Column(String, primary_key=True)
    used_credits = Column(Integer)
    new_records = Column(Integer)
    deleted_records = Column(Integer)
    updated_records = Column(Integer)
