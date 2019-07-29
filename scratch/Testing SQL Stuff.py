from sqlalchemy import create_engine, inspect, or_, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, DateTime, Date, Integer, BigInteger, Numeric, Boolean
import sqlalchemy_utils as sql_utils
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime as dt
import itertools
import collections
import string
import random
import yaml


def constr_postgres(uid, pwd, host, port, dbname, kwargs=None):
    # postgresql+psycopg2://user:password@host:port/dbname
    constr = f'postgresql+psycopg2://{uid}:{pwd}@{host}:{port}/{dbname}'

    return constr

SQLBASE = declarative_base()


class Prices(SQLBASE):
    __tablename__ = 'dev_security_prices'
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


letters = [i for i in string.ascii_uppercase]
n = 1000
dataset = collections.OrderedDict()
symbols = [random.sample(letters, random.choice(range(2, 4))) for _ in range(n)]
symbols = [''.join(i) for i in symbols]

dataset['symbol'] = symbols
dataset['exchange'] = [random.choice(['AMEX', 'NASDAQ', 'NYSE']) for _ in range(n)]
dataset['date'] = list(pd.date_range(end='2018-01-01', periods=n).to_pydatetime())
dataset['open'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]
dataset['high'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]
dataset['low'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]
dataset['close'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]
dataset['volume'] = [random.choice(range(1000000)) for _ in range(n)]
dataset['adjusted_close'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]
dataset['dividend_amount'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]
dataset['split_coefficient'] = [round(random.uniform(5, 9999), 2) for _ in range(n)]

dataset = pandas.DataFrame(dataset)
dataset_recs = dataset.to_dict('records')

with open('../config/config.yaml', 'r') as f:
    CONFIG = yaml.load(f)

knowsec_creds = CONFIG['dbserver']
knowsec_creds['dbname'] = 'knowsec'

constr = constr_postgres(**knowsec_creds)

dbengine = create_engine(constr, echo=False)
dbsession_factory = sessionmaker(bind=dbengine)
dbsession = dbsession_factory(autocommit=False)


def insert_one(dataset):
    for record in dataset_recs:
        dbsession.add(Prices(**record))


def insert_pandas(dataset):
    dataset.to_sql('dev_security_prices', dbengine, if_exists='append', index=False)


def insert_bulk(dataset):
    dbsession.bulk_insert_mappings(Prices, dataset_recs)


%timeit -n5 -r10 insert_one(dataset_recs)
%timeit -n5 -r10 insert_pandas(dataset)
%timeit -n5 -r10 insert_bulk(dataset_recs)
