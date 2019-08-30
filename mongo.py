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
        for chunkindex, chunk in enumerate(uproot.iterate(files, 'Events', entrysteps=50000, namedecode='ascii')):
            for column, data in chunk.items():
                if not isinstance(data, np.ndarray) or len(data.shape) != 1:
                    print("Skpping column %s in chunk %d of %s" % (column, chunkindex, dataset))
                data = db.test.insert_one(pack(data))
                record = {
                    'dataset': dataset,
                    'chunkindex': chunkindex,
                    'columns': {
                        column: data.inserted_id,
                    }
                }
                db.test.insert_one(record)


def test(db):
    allcolumns = {'MetYCorrjesDown', 'neleLoose', 'AK4Puppijet1_dR08', 'AK4Puppijet1_dPhi08', 'AK8Puppijet0_msd', 'genVPt', 'AK4Puppijet2_deepcsvb', 'AK8Puppijet0_phi', 'AK8Puppijet1_tau32', 'MetXCorrjesUp', 'AK4Puppijet1_deepcsvb', 'MetYCorrjesUp', 'AK8Puppijet0_deepdoubleb', 'AK8Puppijet1_e4_v2_sdb1', 'vmuoLoose0_pt', 'AK8Puppijet0_pt_JESDown', 'AK4Puppijet1_deepcsvbb', 'AK4Puppijet0_dR08', 'nmuLoose', 'scale1fb', 'AK8Puppijet0_isHadronicV', 'AK4Puppijet3_dR08', 'MetXCorrjerDown', 'AK4Puppijet3_dPhi08', 'AK4Puppijet3_deepcsvb', 'MetXCorrjesDown', 'AK4Puppijet2_dPhi08', 'genVPhi', 'AK4Puppijet0_pt', 'AK4Puppijet3_deepcsvbb', 'runNum', 'MetYCorrjerUp', 'AK8Puppijet0_pt_JERUp', 'npu', 'AK8Puppijet0_pt', 'AK4Puppijet2_dR08', 'AK8Puppijet1_phi', 'AK4Puppijet0_deepcsvb', 'AK8Puppijet0_deepdoublec', 'AK4Puppijet0_deepcsvbb', 'AK4Puppijet2_pt', 'AK8Puppijet0_pt_JERDown', 'AK8Puppijet0_pt_JESUp', 'AK4Puppijet1_pt', 'vmuoLoose0_phi', 'AK8Puppijet0_isTightVJet', 'pfmet', 'MetXCorrjerUp', 'ntau', 'pfmetphi', 'AK8Puppijet1_e3_v1_sdb1', 'genVMass', 'AK8Puppijet0_eta', 'AK8Puppijet1_msd', 'AK4Puppijet0_dPhi08', 'AK8Puppijet0_N2sdb1', 'MetYCorrjerDown', 'AK4Puppijet3_pt', 'AK4Puppijet2_deepcsvbb', 'vmuoLoose0_eta', 'nAK4PuppijetsPt30', 'AK8Puppijet0_deepdoublecvb'}
    pipeline = [
        {'$match': {'$or': [
            {'columns.%s' % column: {'$exists': True}} for column in allcolumns
        ]}},
        {'$group': {'_id': {'dataset': '$dataset', 'chunkindex': '$chunkindex'}, 'columns': { '$mergeObjects': '$columns' }}},
    ]
    print(pipeline)
    cur = db.test.aggregate(pipeline)

    for chunk in cur:
        print(chunk['_id'])
        columns = chunk['columns']
        for col in columns.keys():
            data = db.test.find_one(columns[col])
            columns[col] = unpack(data)

        # do the stuff
        print(columns)



client = MongoClient('localhost:27017')
db = client.coffeadb

load(db)
test(db)
