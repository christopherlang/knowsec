import tqdm
import pandas
import functools
import os
import csv


for dirpath, dirnames, filenames in os.walk('../data/historicals'):
    files = [os.path.join(dirpath, i) for i in filenames]


def read_file(filename):
    data = pandas.read_csv(filename, sep='\t', encoding='utf-8')
    data['Datetime'] = pandas.to_datetime(data['Datetime'], utc=True,
                                          format='%Y-%m-%d %H:%M:%S')
    data = data.set_index(['Symbol', 'Datetime'])

    return data


def csv_reducer(filename1, filename2):
    return filename1.append(filename2)


files_pb = tqdm.tqdm(files, ncols=80)
data = [read_file(i) for i in files_pb]

with open('../data/historicals.csv', 'w', encoding='utf-8') as f:
    data[0].to_csv(f, sep='\t', mode='w', encoding='utf-8',
                   date_format='%Y-%m-%dT%H:%M:%S', header=True)

write_pbar = tqdm.tqdm(range(1, len(data)), ncols=80, total=len(data) - 1)
with open('../data/historicals.csv', 'a', encoding='utf-8') as f:
    for i in write_pbar:
        data[i].to_csv(f, sep='\t', mode='a', encoding='utf-8',
                       date_format='%Y-%m-%dT%H:%M:%S', header=False)
