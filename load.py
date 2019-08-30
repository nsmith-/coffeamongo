from pymongo import MongoClient
from bson import Binary
import blosc
import numpy as np
import uproot

def pack(a):
    # assume a.shape = (n,)
    raw = blosc.compress_ptr(a.__array_interface__['data'][0], a.size, a.dtype.itemsize, cname='lz4')
    return {'size': a.size, 'dtype': str(a.dtype), 'data': Binary(raw)}


def unpack(col):
    a = np.empty(col['size'], dtype=col['dtype'])
    blosc.decompress_ptr(bytes(col['data']), a.__array_interface__['data'][0])
    return a


def load(db):
    db.test.drop()

    inputs = {
        'GluGluHToBB_M125_LHEHpT_250_Inf_13TeV_amcatnloFXFX_pythia8': [
            '/Users/ncsmith/src/coffeandbacon/analysis/data/GluGluHToBB_M125_LHEHpT_250_Inf_13TeV_amcatnloFXFX_pythia8.Output_job0_job0_file0to59.root',
        ],
        'GluGluHToCC_M125_LHEHpT_250_Inf_13TeV_amcatnloFXFX_pythia8': [
            '/Users/ncsmith/src/coffeandbacon/analysis/data/GluGluHToCC_M125_LHEHpT_250_Inf_13TeV_amcatnloFXFX_pythia8.Output_job15_job15_file900to959.root',
        ],
        'TTToHadronic_TuneCP5_13TeV_powheg_pythia8': [
            '/Users/ncsmith/src/coffeandbacon/analysis/data/TTToHadronic_TuneCP5_13TeV_powheg_pythia8.Output_job101_job101_file4040to4079.root',
        ],
        'QCD_HT700to1000_TuneCP5_13TeV-madgraphMLM-pythia8': [
            '/Users/ncsmith/src/coffeandbacon/analysis/data/QCD_HT700to1000_TuneCP5_13TeV-madgraphMLM-pythia8.Output_job116_job116_file1160to1169.root',
        ],
    }

    for dataset, files in inputs.items():
        for chunkindex, chunk in enumerate(uproot.iterate(files, 'Events', entrysteps=500000, namedecode='ascii')):
            for column, data in chunk.items():
                if not isinstance(data, np.ndarray) or len(data.shape) != 1:
                    print("Skpping column %s in chunk %d of %s" % (column, chunkindex, dataset))
                record = {
                    'dataset': dataset,
                    'chunkindex': chunkindex,
                    'column': column,
                    'data': pack(data),
                }
                res = db.test.insert_one(record)
                if not res.acknowledged:
                    raise RuntimeError("Failed to write column %s in chunk %d of %s" % (column, chunkindex, dataset))


client = MongoClient('localhost:27017')
db = client.coffeadb

load(db)
