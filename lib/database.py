from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker, load_only
import pandas
from IPython.core.debugger import set_trace


SQLBASE = declarative_base()


class StockDB:
    def __init__(self, filename):
        self._dbengine = create_engine('sqlite:///' + filename, echo=False)
        self._dbsession_factory = sessionmaker(bind=self._dbengine)
        self._dbsession = self._dbsession_factory()

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
