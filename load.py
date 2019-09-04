from pymongo import MongoClient
from urllib.parse import quote_plus
from bson import Binary
import blosc
import numpy as np
import uproot
import json
from tqdm import tqdm
import configparser

def pack(a):
    # assume a.shape = (n,)
    raw = blosc.compress_ptr(a.__array_interface__['data'][0], a.size, a.dtype.itemsize, cname='lz4')
    return {'size': a.size, 'dtype': str(a.dtype), 'data': Binary(raw)}


def unpack(col):
    a = np.empty(col['size'], dtype=col['dtype'])
    blosc.decompress_ptr(bytes(col['data']), a.__array_interface__['data'][0])
    return a


def load(db, collection='test'):
    db[collection].drop()

    with open('samplefiles.json') as fin:
        samplesets = json.load(fin)

    inputs = samplesets['test_skim']

    for dataset, files in inputs.items():
        for chunkindex, chunk in tqdm(enumerate(uproot.iterate(files, 'otree', entrysteps=500000, namedecode='ascii')), desc=dataset, unit='chunk'):
            for column, data in chunk.items():
                if not isinstance(data, np.ndarray) or len(data.shape) != 1:
                    print("Skipping column %s in chunk %d of %s" % (column, chunkindex, dataset))
                record = {
                    'dataset': dataset,
                    'chunkindex': chunkindex,
                    'column': column,
                    'data': pack(data),
                }
                res = db[collection].insert_one(record)
                if not res.acknowledged:
                    raise RuntimeError("Failed to write column %s in chunk %d of %s" % (column, chunkindex, dataset))
                # print("Wrote column %s in chunk %d of %s" % (column, chunkindex, dataset))


config = configparser.ConfigParser()
config.read('config.ini')
uri = 'mongodb://%s:%s@%s/%s.%s' % (quote_plus(config['mongo']['user']),
                                    quote_plus(config['mongo']['password']),
                                    config['mongo']['host'],
                                    config['mongo']['database'],
                                    config['mongo']['collection'],
                                    )
client = MongoClient(uri)
db = client[config['mongo']['database']]

# load(db, config['mongo']['collection'])
