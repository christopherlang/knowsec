import os
# import sys
import yaml
from lib import dfu
from lib import database


os.chdir('C:/Users/Christopher Lang/Documents/projects/knowsec')

CONFIG = dict()
with open('config/data_config.yaml', 'r') as f:
    CONFIG['data'] = yaml.load(f)

db = database.StockDB('db/storage.db')

# Perform company table update
comp_table = list()
for exchange in ['NYSE', 'NASDAQ', 'AMEX']:
    comp_table.append(dfu.get_exchange(exchange))

comp_table = comp_table[0].append(comp_table[1]).append(comp_table[2])

db.update_company(comp_table)

tsdata = dfu.TimeSeriesData()

