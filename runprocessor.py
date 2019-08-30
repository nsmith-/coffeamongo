import pyspark.sql.types as types
import pyspark.sql.functions as fn
import lz4.frame as lz4f
import pickle
import blosc
import time
import numpy
from pyspark.sql import SparkSession
from functools import partial
from coffea import processor, util


def unpack(col):
    a = numpy.empty(col['size'], dtype=col['dtype'])
    blosc.decompress_ptr(bytes(col['data']), a.__array_interface__['data'][0])
    return a


def zipadd(a, b):
    out = pickle.loads(lz4f.decompress(a))
    out += pickle.loads(lz4f.decompress(b))
    return lz4f.compress(pickle.dumps(out), compression_level=1)


def process_chunk(processor_instance, chunkid, columns):
    columns = columns.asDict()
    size = None
    print(chunkid)
    for col in columns.keys():
        if columns[col] is None:
            print("Column %r is none" % col)
            continue
        pass
        # columns[col] = unpack(columns[col])
        # if not (size is None or size == len(columns[col])):
        #     raise RuntimeError("column size mismatch")

    # print(size)
    # df = processor.PreloadedDataFrame(size=size, items=columns)
    # df['dataset'] = chunkid['dataset']
    # out = processor_instance.process(df)
    out = "asdfasdf"
    return lz4f.compress(pickle.dumps(out), compression_level=1)


def execute(spark, processor_instance, pipeline):
    spark_udf = fn.udf(partial(process_chunk, processor_instance), types.BinaryType())
    db = spark.read.format("mongo")
    blob = (db
            .option("pipeline", pipeline).load()
            .select(spark_udf('_id', 'columns').alias('blob'))
            .rdd.map(lambda row: row['blob'])
            .treeReduce(zipadd, depth=2)
            )
    return pickle.loads(lz4f.decompress(blob))


def build_pipeline(columns):
    pipeline = r"""[
        {$match: {$or: [
    """
    for col in columns:
        pipeline += "{'columns.%s': {$exists: true}},\n" % col
    pipeline += """
            ]},
        },
        {$group: {'_id': {'dataset': '$dataset', 'chunkindex': '$chunkindex'}, 'columns': {$mergeObjects: '$columns' }}},
    ]"""
    return pipeline


spark = (SparkSession.builder
    .appName("myApp")
    .config('spark.sql.execution.arrow.enabled','true')
    .config('spark.sql.execution.arrow.fallback.enabled','false')
    .config('spark.sql.execution.arrow.maxRecordsPerBatch', 200000)
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/coffeadb.test")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/coffeadb.test")
    .getOrCreate()
)


corrections = util.load('corrections.coffea')
allcolumns = {'MetYCorrjesDown', 'neleLoose', 'AK4Puppijet1_dR08', 'AK4Puppijet1_dPhi08', 'AK8Puppijet0_msd', 'genVPt', 'AK4Puppijet2_deepcsvb', 'AK8Puppijet0_phi', 'AK8Puppijet1_tau32', 'MetXCorrjesUp', 'AK4Puppijet1_deepcsvb', 'MetYCorrjesUp', 'AK8Puppijet0_deepdoubleb', 'AK8Puppijet1_e4_v2_sdb1', 'vmuoLoose0_pt', 'AK8Puppijet0_pt_JESDown', 'AK4Puppijet1_deepcsvbb', 'AK4Puppijet0_dR08', 'nmuLoose', 'scale1fb', 'AK8Puppijet0_isHadronicV', 'AK4Puppijet3_dR08', 'MetXCorrjerDown', 'AK4Puppijet3_dPhi08', 'AK4Puppijet3_deepcsvb', 'MetXCorrjesDown', 'AK4Puppijet2_dPhi08', 'genVPhi', 'AK4Puppijet0_pt', 'AK4Puppijet3_deepcsvbb', 'runNum', 'MetYCorrjerUp', 'AK8Puppijet0_pt_JERUp', 'npu', 'AK8Puppijet0_pt', 'AK4Puppijet2_dR08', 'AK8Puppijet1_phi', 'AK4Puppijet0_deepcsvb', 'AK8Puppijet0_deepdoublec', 'AK4Puppijet0_deepcsvbb', 'AK4Puppijet2_pt', 'AK8Puppijet0_pt_JERDown', 'AK8Puppijet0_pt_JESUp', 'AK4Puppijet1_pt', 'vmuoLoose0_phi', 'AK8Puppijet0_isTightVJet', 'pfmet', 'MetXCorrjerUp', 'ntau', 'pfmetphi', 'AK8Puppijet1_e3_v1_sdb1', 'genVMass', 'AK8Puppijet0_eta', 'AK8Puppijet1_msd', 'AK4Puppijet0_dPhi08', 'AK8Puppijet0_N2sdb1', 'MetYCorrjerDown', 'AK4Puppijet3_pt', 'AK4Puppijet2_deepcsvbb', 'vmuoLoose0_eta', 'nAK4PuppijetsPt30', 'AK8Puppijet0_deepdoublecvb'}

from boostedHbbProcessor import BoostedHbbProcessor
hbb = BoostedHbbProcessor(corrections=corrections,
                          columns=allcolumns,
                          )
start = time.time()
out = execute(spark, hbb, build_pipeline(hbb.columns))
print("Total execution time: %f" % (time.time() - start))
