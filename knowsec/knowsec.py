import os
# import sys
import yaml
import dfu
import database
import importlib
from datetime import datetime as dt
import pytz
import time
import random

# os.chdir('C:/Users/Christopher Lang/Documents/projects/knowsec')
# os.chdir('/Users/christopherlang/Documents/projects/knowsec')
CONFIG = dict()
with open('../config/data_config.yaml', 'r') as f:
    CONFIG['data'] = yaml.load(f)

db = database.StockDB('../db/storage.db')

db.delete_table('eod_stockprices')
db.create_table('eod_stockprices')

# Perform company table update
comp_table = list()
for exchange in ['NYSE', 'NASDAQ', 'AMEX']:
    comp_table.append(dfu.get_exchange(exchange))

comp_table = comp_table[0].append(comp_table[1]).append(comp_table[2])

db.update_company(comp_table)

tsdata = dfu.TimeSeriesData()


for i in range(100):
    rec_dt = dt(random.sample(range(1000, 2020), 1)[0],
                random.sample(range(1, 13), 1)[0],
                random.sample(range(1, 29), 1)[0])
    rec = {
        'Symbol': comp_table['Symbol'][i],
        'Datetime': pytz.timezone('UTC').localize(rec_dt),
        'open': random.uniform(30, 100),
        'high': random.uniform(30, 100),
        'close': random.uniform(30, 100),
        'volume': 104583729
    }

    db.append_record('eod_stockprices', rec)

    print(i)

db.append_record('eod_stockprices', rec)