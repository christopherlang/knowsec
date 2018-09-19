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

stock_data = dfu.AlphaAdvantage()

tmp = stock_data.retrieve_data('AMD', output='full')
tmp = dfu.standardize_stock_series(tmp)

db.bulk_insert_records('eod_stockprices', tmp)
