from pyspark.sql import SparkSession, Row
import pyspark.sql.types as types
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
import pyspark.sql.functions as fn
import blosc
import numpy as np
import awkward

def pack(a):
    # assume a.shape = (n,)
    raw = blosc.compress_ptr(a.__array_interface__['data'][0], a.size, a.dtype.itemsize)
    return {'size': a.size, 'dtype': str(a.dtype), 'data': raw}

def unpack(col):
    a = np.empty(col['size'], dtype=col['dtype'])
    blosc.decompress_ptr(bytes(col['data']), a.__array_interface__['data'][0])
    return a

columntype = types.StructType([
    types.StructField("size", types.IntegerType(), False),
    types.StructField("dtype", types.StringType(), False),
    types.StructField("data", types.BinaryType(), False),
])

columnrecordtype = types.StructType([
    types.StructField("dataset", types.StringType(), False),
    types.StructField("chunkindex", types.IntegerType(), False),
    types.StructField("columns", types.StructType([
        types.StructField("derived", columntype, False),
    ]), False),
])

@udf(columnrecordtype)
def process_chunk(chunkid, columns):
    print(chunkid)
    columns = columns.asDict()
    for col in columns.keys():
        columns[col] = unpack(columns[col])

    record = {
        'dataset': chunkid['dataset'],
        'chunkindex': chunkid['chunkindex'],
        'columns': {
            'derived': pack(columns['AK4Puppijet1_dPhi08'] + columns['nphoLoose']),
        }
    }
    return record


def flatten_schema(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, types.ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, types.StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

rowtype = types.StructType([
    types.StructField("dataset", types.StringType(), False),
    types.StructField("AK4Puppijet1_dPhi08", types.FloatType(), False),
    types.StructField("nphoLoose", types.IntegerType(), False),
    types.StructField("derived", types.FloatType(), False),
])
# types.from_arrow_schema()

@pandas_udf(rowtype, PandasUDFType.GROUPED_MAP)
def expand_chunk(chunk):
    chunk = dict(chunk.iloc[0])
    dataset = chunk.pop('_id_dataset')
    chunkindex = chunk.pop('_id_chunkindex')
    columnnames = set(k[8:-5] for k in chunk.keys() if k.startswith('columns_') and k.endswith('_data'))
    columns = {}
    for name in columnnames:
        columns[name] = unpack({
            'size': chunk['columns_%s_size' % name],
            'dtype': chunk['columns_%s_dtype' % name],
            'data': chunk['columns_%s_data' % name],
        })

    table = awkward.Table(columns)
    table = awkward.toarrow(table).to_pandas()
    table['dataset'] = dataset
    return table


spark = (SparkSession.builder
    .appName("myApp")
    .config('spark.sql.execution.arrow.enabled','true')
    .config('spark.sql.execution.arrow.maxRecordsPerBatch', 200000)
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/coffeadb.test")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/coffeadb.test")
    .getOrCreate()
)


pipeline = """[
    {$match: {$or: [
            {'columns.nphoLoose': {$exists: true}},
            {'columns.AK4Puppijet1_dPhi08': {$exists: true}},
        ]},
    },
    {$group: {'_id': {'dataset': '$dataset', 'chunkindex': '$chunkindex'}, 'columns': {$mergeObjects: '$columns' }}},
]"""

df = spark.read.format("mongo").option("pipeline", pipeline).load()
newdf = df.select(process_chunk('_id', 'columns').alias('record')).select('record.*')
newdf.write.format("mongo").mode("append").save()


pipeline = """[
    {$match: {$or: [
        {'columns.nphoLoose': {$exists: true}},
        {'columns.AK4Puppijet1_dPhi08': {$exists: true}},
        {'columns.derived': {$exists: true}},
    ]}},
    {$group: {'_id': {'dataset': '$dataset', 'chunkindex': '$chunkindex'}, 'columns': {$mergeObjects: '$columns' }}},
]"""
df = spark.read.format("mongo").option("pipeline", pipeline).load()
flat = df.select([fn.col(col).alias(col.replace('.', '_')) for col in flatten_schema(df.schema)])
newdf = flat.groupby(['_id_dataset', '_id_chunkindex']).apply(expand_chunk)
p = newdf.limit(10).toPandas()
print(p)
