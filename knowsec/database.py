from sqlalchemy import create_engine, inspect, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy.orm import sessionmaker, load_only
import pandas
from IPython.core.debugger import set_trace


SQLBASE = declarative_base()


class StockDB:
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
        if self._autocommit is True:
            pass
        else:
            self._dbsession.commit()

    def list_tables(self):
        return self._dbengine.table_names()

    def table_schema(self, tablename):
        return inspect(self._dbengine).get_columns(tablename)

    def create_table(self, tablename):
        SQLBASE.metadata.create_all(self._dbengine)

    def delete_table(self, tablename):
        table_mapper(tablename).__table__.drop(self._dbengine)

    def clear_records(self, tablename):
        self._dbsession.query(table_mapper(tablename)).delete()

    def retrieve_prices(self, symbols=None, period='EOD', startdate=None,
                        enddate=None):
        table_obj = None
        if period == 'EOD':
            table_obj = EODPrices

        elif period == 'intraday':
            pass

        else:
            raise ValueError("param: period must be either 'EOD', 'intraday'")

        query_obj = self._dbsession.query(table_obj)

        symbol_list = list()
        if isinstance(symbols, str):
            symbol_list.append(symbols)
        else:
            symbol_list = symbols

        if symbols is not None:
            query_obj = query_obj.filter(table_obj.Symbol.in_(symbol_list))

        if startdate is not None:
            query_obj = query_obj.filter(table_obj.Datetime >= startdate)

        if enddate is not None:
            query_obj = query_obj.filter(table_obj.Datetime <= enddate)

        result = pandas.read_sql(query_obj.statement, query_obj.session.bind)
        result = result.set_index(['Symbol', 'Datetime'])

        return result

    def insert_record(self, tablename, record):
        if isinstance(record, dict):
            rec_insert = table_mapper(tablename)(**record)

            self._dbsession.add(rec_insert)

        elif isinstance(record, pandas.core.frame.DataFrame):
            record.to_sql(tablename, self._dbengine, if_exists='append',
                          index=True)

        else:
            raise TypeError('param: record should be a dict, pandas')

    def bulk_insert_records(self, tablename, records):
        if isinstance(records, list):
            table_obj = table_mapper(tablename)
            row_recs = [table_obj(**i) for i in records]
            self._dbsession.bulk_insert_records(row_recs)

        elif isinstance(records, pandas.core.frame.DataFrame):
            records.to_sql(tablename, self._dbengine, if_exists='append',
                           index=True)

    def update_company(self, dataframe):
        dataframe.to_sql('company', self._dbengine, if_exists='replace',
                         index=False)

    def retrieve_company(self, columns=None, symbol=None):
        df_com = None

        if columns is None:
            df_com = pandas.read_sql('company', self._dbengine)
        else:
            if isinstance(columns, str):
                columns = [columns]

                if 'Symbol' not in columns:
                    columns.insert(0, 'Symbol')

            else:
                if 'Symbol' not in columns:
                    columns = ['Symbol'] + columns

            df_com = pandas.read_sql('company', self._dbengine,
                                     columns=columns)

        if symbol is not None:
            if isinstance(symbol, str):
                symbol = [symbol]

            df_com = df_com[df_com['Symbol'].isin(symbol)]

        return df_com

    # def create_stockts(self):

    # def set_prices(self, dataframe):
    #     dataframe.to_sql('eod_stockprices', self._dbengine,
    #                      if_exists='replace', index=True)


def table_mapper(tablename):
    result = None

    if tablename == 'eod_stockprices':
        result = EODPrices
    elif tablename == 'company':
        result = Company
    else:
        raise ValueError('param: tablename is not a valid table name')

    return result


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
