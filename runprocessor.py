import pyspark.sql.types as types
import pyspark.sql.functions as fn
import lz4.frame as lz4f
import pickle
import blosc
import time
import numpy
import pandas as pd
from pyspark.sql import SparkSession
from functools import partial
from coffea import processor, util
import configparser


def unpack(col):
    a = numpy.empty(col['size'], dtype=col['dtype'])
    blosc.decompress_ptr(bytes(col['data']), a.__array_interface__['data'][0])
    return a


def zipadd(a, b):
    out = pickle.loads(lz4f.decompress(a))
    out += pickle.loads(lz4f.decompress(b))
    return lz4f.compress(pickle.dumps(out), compression_level=1)


def process_chunk_pivot(processor_instance, chunkid, chunk):
    dataset, chunkindex = chunkid
    columns = {}
    size = None
    for _, row in chunk.iterrows():
        colname = row['column']
        columns[colname] = unpack(row)
        if not (size is None or size == len(columns[colname])):
            raise RuntimeError("column size mismatch in chunk %r column %s" % (chunkid, colname))
        elif size is None:
            size = len(columns[colname])

    df = processor.PreloadedDataFrame(size=size, items=columns)
    df['dataset'] = dataset
    out = processor_instance.process(df)
    out = lz4f.compress(pickle.dumps(out), compression_level=1)
    return pd.DataFrame([{'blob': out}])


def execute(spark, processor_instance, pipeline):
    blobrow = types.StructType([types.StructField("blob", types.BinaryType(), False)])
    spark_udf = fn.pandas_udf(partial(process_chunk_pivot, processor_instance), blobrow, fn.PandasUDFType.GROUPED_MAP)
    db = spark.read.format("mongo")
    blob = (db
            .option("pipeline", pipeline).load()
            .select(['dataset', 'chunkindex', 'column', 'data.*'])
            .groupby(['dataset', 'chunkindex'])
            .apply(spark_udf)
            .rdd.map(lambda row: row['blob'])
            .treeReduce(zipadd, depth=2)
            )
    return pickle.loads(lz4f.decompress(blob))


def build_pipeline(columns):
    pipeline = "[\n {$match: {$or: [\n"
    for col in columns:
        pipeline += "            {'column': %r},\n" % col
    pipeline += """
            ]},
        },
    ]"""
    return pipeline


config = configparser.ConfigParser()
config.read('config.ini')
uri = 'mongodb://%s:%s@%s/%s.%s' % (quote_plus(config['mongo']['user']),
                                    quote_plus(config['mongo']['password']),
                                    config['mongo']['host'],
                                    config['mongo']['database'],
                                    config['mongo']['collection'],
                                    )

spark = (SparkSession.builder
         .master(config['spark']['master'])
         .appName("mongo")
         .config('spark.sql.execution.arrow.enabled','true')
         .config('spark.sql.execution.arrow.fallback.enabled','false')
         .config('spark.sql.execution.arrow.maxRecordsPerBatch', 200000)
         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
         .config("spark.mongodb.input.uri", uri)
         .config("spark.mongodb.output.uri", uri)
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
util.save(out, "hists.coffea")
