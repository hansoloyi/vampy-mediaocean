"""
This module creates a collection function that can be re-used to ingest and process data files from the mediaocean-sftp
"""
import json
import functools
import argparse
import re
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.window import Window
from urllib.parse import urlparse

url_env = os.environ['DATABASE_API_CONNECTION_MEDIAOCEAN']
parsed_config = urlparse(url_env)
url = f"jdbc:postgresql://{parsed_config.hostname}:{parsed_config.port}{parsed_config.path}"
properties = {
    "driver": "org.postgresql.Driver",
    "user": parsed_config.username,
    "password": parsed_config.password
}

def get_spark_session(
        name='template',
        spark_home="/opt/spark",
        python_path="/opt/conda/bin/python",
        max_cores=10,
        memory=12,
        executor_cores=5,
        storageFraction=.5,
        memoryFraction=.6,
        parallelism=1024,
        memory_per_core=3,
        ip = "spark-dev-master-001.prod.dc3",
        dynamic_overwrite=True
    ):

    """
    Initialize a spark session.
    Arguments:
        name: string
            Spark application name
        max_cores: int
            maximum number of cpu cores to allocate to the
            application (see spark.cores.max). This should be set to a
            multiple of 10 for best utilization of the dc3 cluster when
            multiple applications are running.
        memory: int
            heap memory
        executor_cores: int
            number of cores for each executor
        storageFraction: float
            storage fraction for both heap and off-heap memory
        memoryFraction: float
            Fraction of (heap space - 300MB) used for execution and storage
        parallelism: int
            would set 'shuffle.partitions' and 'default.parallelism'
        memory_per_core: int
            amount of memory avaiable to each core in GB(total memory of machine/number of cores)
        ip: string
            the url of the spark master
    """

    print("spark initiating ...")

    total_memory = int(memory_per_core * executor_cores)
    off_heap = int(total_memory - memory)

    os.environ["SPARK_HOME"] = spark_home
    os.environ["PYSPARK_PYTHON"] = python_path

    if memory > total_memory:
        print("heap memory is out of bound")
        raise ValueError(f"memory(={memory}) has to be smaller than {total_memory}")

    executorFlags =   ["-XX:+UseParallelGC" ,
      "-XX:+HeapDumpOnOutOfMemoryError" ,
      "-XX:HeapDumpPath=/scratch1/heapdumps/" ,
      "-XX:+PrintClassHistogram" ]


    conf = SparkConf().setAppName(name)
    conf.set("spark.ui.showConsoleProgress", "true")
    conf.set("spark.executor.cores", executor_cores)
    conf.set('spark.cores.max', max_cores)
    conf.set("spark.default.parallelism", str(parallelism))
    conf.set("spark.kryoserializer.buffer.max", "2040m")
    conf.set('spark.executor.memory','{}g'.format(memory))
    conf.set('spark.driver.memory','32g')
    conf.set('spark.driver.maxResultSize', '16g')
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.executor.extraJavaOptions", " ".join(executorFlags))
    conf.set("spark.shuffle.io.preferDirectBufs", "true")
    conf.set("spark.rpc.io.preferDirectBufs", "true")
    conf.set("spark.sql.shuffle.partitions", str(parallelism))
    conf.set("spark.memory.storageFraction", storageFraction)
    conf.set("spark.memory.fraction", memoryFraction)
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.jars", "/home/spark/jars/postgresql-42.2.2.jar")
    if dynamic_overwrite:
        conf.set("spark.sql.sources.partitionOverwriteMode","DYNAMIC")

    if off_heap > 0:
        print(f"heap is on with {off_heap}GB")
        conf.set('spark.memory.offHeap.enabled', 'true')
        conf.set('spark.memory.offHeap.size',  off_heap * 1024 * 1024 * 1024)

    spark = (
        SparkSession.builder
        .master("spark://{}:7077".format(ip))
        .config(conf = conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


spark_session = get_spark_session(
    name="ingest_from_SFTP",
    max_cores=64,
    memory_per_core=3.75,
    memory=20,
    executor_cores=8,
    parallelism=256,
    memoryFraction=1,
    ip="spark-compute-master-001.prod.dc3"
)


def create_window(cols):
    partition_cols = list(filter(lambda c: c is not "name", cols))
    return Window().partitionBy(*partition_cols).orderBy(F.col("extract_datetime").asc())


def filter_for_feed(files, feed, ds=0):
    """
    Filters a list of files that need to be ingested by feed
    :param files: list of files that need to be ingested
    :param feed: feed type
    :return: filtered list of files based on feed
    """

    return list(filter(lambda x: (feed.upper() in x.upper()) and (int(re.search("([0-9]{8})", x).group()) > ds), files))


def run_ingest(files, feed_type):
    f = filter_for_feed(files, feed_type)

    funcs = {
        'ADVERTISERFEED': run_advertiser_feed,
        'DESTINATIONFEED': run_destination_feed,
        'ESTIMATEFEED': run_estimate_feed,
        'DAYPARTFEED': run_daypart_feed,
        'PROGRAMFEED': run_program_feed
    }

    run_func = funcs[feed_type]
    run_func(f)


def create_tenant_df():
    return (read_df_from_postgres("mediaocean.tenant", 2)
            .withColumnRenamed("id", "tenant_id")
            .withColumnRenamed("mo_id", "tenant_mo_id"))


def run_advertiser_feed(advertiser_files):
    print('...advertisers...')
    client_cols = ["mo_id", "name", "tenant_id"]
    product_cols = ["mo_id", "name", "client_id"]

    advertisers_df = create_advertiser_df_from_sftp_file(advertiser_files)
    tenants_df = create_tenant_df()

    joined_df = (advertisers_df
                 .join(tenants_df, F.col("tenant") == F.col("tenant_mo_id"), "left_outer")
                 .cache())

    non_tenants = joined_df.filter(F.col("tenant_id").isNull()).select("tenant").distinct()

    if len(non_tenants.take(1)) > 0:
        missing_tenant_files = "\n".join([str(row.tenant) for row in non_tenants.collect()])
        raise Exception(f"Tenants missing: {missing_tenant_files}")

    # -- runs client table --
    w = create_window(["tenant_id", "advertiser_id"])
    client_df = (joined_df
                 .withColumn("rn", F.row_number().over(w))
                 .withColumnRenamed("advertiser_id", "mo_id")
                 .withColumnRenamed("advertiser_name", "name")
                 .filter(F.col("rn") == 1)
                 .select(*client_cols))
    write_df_to_postgres(client_df, "mediaocean.client")

    # -- runs product table --
    client_read_df = read_df_from_postgres("mediaocean.client", 5).withColumnRenamed("id", "client_id")
    product_df = joined_df.select("advertiser_id", "brand_id", "brand_name", "extract_datetime").distinct()

    pw = create_window(["tenant_id", "brand_id"])
    product_final_df = (product_df
                        .join(client_read_df, F.col("advertiser_id") == F.col("mo_id"), "left_outer")
                        .withColumn("rn", F.row_number().over(pw))
                        .drop("mo_id", "name")
                        .withColumnRenamed("brand_id", "mo_id")
                        .withColumnRenamed("brand_name", "name")
                        .filter(F.col("rn") == 1)
                        .select(*product_cols))
    write_df_to_postgres(product_final_df, "mediaocean.product")


def run_destination_feed(destination_files):
    print('...destination...')
    network_cols = ["mo_id", "name", "tenant_id", "media", "network_type", "nielsen_code"]
    window_cols = ["mo_id", "tenant_id"]
    dest_df = create_destination_df_from_sftp_file(destination_files)
    tenants_df = create_tenant_df()

    joined_df = dest_df.join(tenants_df, F.col("tenant") == F.col("tenant_mo_id"), "left_outer")
    non_tenants = joined_df.filter(F.col("tenant_id").isNull()).select("tenant").distinct()
    if len(non_tenants.take(1)) > 0:
        missing_tenant_files = "\n".join([str(row.tenant) for row in non_tenants.collect()])
        raise Exception(f"Tenants missing: {missing_tenant_files}")

    w = create_window(window_cols)
    network_df = (joined_df
                  .drop("mo_id")
                  .withColumnRenamed("id", "mo_id")
                  .withColumnRenamed("type", "network_type")
                  .withColumn("rn", F.row_number().over(w))
                  .filter(F.col("rn") == 1)
                  .filter(F.col("network_type").isNotNull())
                  .select(*network_cols))
    write_df_to_postgres(network_df, "mediaocean.network")


def run_estimate_feed(estimate_files):
    print('...estimate...')
    estimate_cols = ["mo_id", "media", "client_id", "start_date", "end_date", "name"]
    window_cols = ["mo_id", "client_id"]
    estimate_df = create_estimate_df_from_sftp_file(estimate_files)
    client_read_df = read_df_from_postgres("mediaocean.client", 5).withColumnRenamed("id", "client_id")

    joined_df = estimate_df.join(client_read_df, F.col("advertiser_id") == F.col("mo_id"), "left_outer")
    non_joined = joined_df.filter(F.col("client_id").isNull()).select("name").distinct()
    if len(non_joined.take(1)) > 0:
        missing_clients = "\n".join([str(row.name) for row in non_joined.collect()])
        raise Exception(f"Clients missing: {missing_clients}")

    w = create_window(window_cols)
    estimate_df = (joined_df
                   .drop("mo_id", "name")
                   .withColumnRenamed("id", "mo_id")
                   .withColumnRenamed("estimate_name", "name")
                   .withColumn("rn", F.row_number().over(w))
                   .filter(F.col("rn") == 1)
                   .select(*estimate_cols))
    write_df_to_postgres(estimate_df, "mediaocean.estimate")


def run_daypart_feed(daypart_files):
    print('...daypart...')
    daypart_cols = ["mo_id", "media", "client_id", "name", "tenant_id"]
    window_cols = ["mo_id", "client_id", "tenant_id"]

    daypart_df = create_daypart_df_from_sftp_file(daypart_files)
    tenants_df = create_tenant_df()
    joined_df_tenant = daypart_df.join(tenants_df, F.col("tenant") == F.col("tenant_mo_id"), "left_outer")
    non_joined_tenant = joined_df_tenant.filter(F.col("tenant_id").isNull()).select("tenant").distinct()
    if len(non_joined_tenant.take(1)) > 0:
        missing_tenant_files = "\n".join([str(row.tenant_mo_id) for row in non_joined_tenant.collect()])
        raise Exception(f"Tenants missing: {missing_tenant_files}")

    client_read_df = read_df_from_postgres("mediaocean.client", 5).withColumnRenamed("id", "client_id").withColumnRenamed("mo_id", "client_mo_id")
    joined_df = daypart_df.join(client_read_df, F.col("advertiser_id") == F.col("client_mo_id"), "left_outer")
    w = create_window(window_cols)
    daypart_df_final = (joined_df
                        .drop("name")
                        .withColumnRenamed("id", "mo_id")
                        .withColumnRenamed("daypart_name", "name")
                        .withColumn("rn", F.row_number().over(w))
                        .filter(F.col("rn") == 1)
                        .select(*daypart_cols))
    write_df_to_postgres(daypart_df_final, "mediaocean.daypart")


def run_program_feed(program_files):
    print('...program...')
    program_df = create_program_df_from_sftp_file(program_files)
    program_cols = ["tenant_id", "network_id", "mo_id", "name", "media", "start_date", "end_date"]
    window_cols = ["mo_id", "tenant_id"]

    tenants_df = create_tenant_df()
    network_df = (read_df_from_postgres("mediaocean.network", 5)
                  .withColumnRenamed("id", "network_id")
                  .withColumnRenamed("mo_id", "network_mo_id")
                  .select("network_id", "network_mo_id"))
    tenant_join_df = program_df.join(tenants_df, F.col("tenant") == F.col("tenant_mo_id"), "left_outer")

    non_joined_tenant = tenant_join_df.filter(F.col("tenant_id").isNull()).select("tenant").distinct()
    if len(non_joined_tenant.take(1)) > 0:
        missing_tenant_files = "\n".join([str(row.tenant_mo_id) for row in non_joined_tenant.collect()])
        raise Exception(f"Tenants missing: {missing_tenant_files}")
    network_join_df = tenant_join_df.join(network_df, F.col("program_network_id") == F.col("network_mo_id"), "left_outer")
    # non_join_network = network_join_df.filter(F.col("network_id").isNull()).select("program_network_id").distinct()
    # if len(non_join_network.take(1)) > 0:
    #     missing_network = "\n".join([str(row.program_network_id) for row in non_join_network.collect()])
    #     raise Exception(f"Network missing: {missing_network}")

    w = create_window(window_cols)
    network_final_df = (network_join_df
                        .filter(F.col("network_id").isNotNull())
                        .withColumnRenamed("program_name", "name")
                        .withColumnRenamed("program_media", "media")
                        .withColumnRenamed("program_start_date", "start_date")
                        .withColumnRenamed("program_end_date", "end_date")
                        .withColumnRenamed("program_id", "mo_id")
                        .withColumn("rn", F.row_number().over(w))
                        .filter(F.col("rn") == 1)
                        .select(*program_cols))
    write_df_to_postgres(network_final_df, "mediaocean.program")


# need this temporarily until spark can read directly from efs
def sftp_json_to_rdd(spark_session, filepath):
    '''
    This function reads in a json-file from the efs-mediaocean-sftp and returns it as an rdd
    example pythonic path: '/efs-mediaocean/mediaocean/DS-OO-NATIONAL-ADVERTISERFEED-20171217-100000'

    :param filepath: pythonic-mediaocean-efs file path
    :return: json_rdd: an rdd representation of the json-file
    '''

    with open(filepath) as data_file:
        data = json.load(data_file)

    sc = spark_session.sparkContext
    json_string = json.dumps(data)
    json_rdd = sc.parallelize([json_string])

    return json_rdd


def create_df(schema, file, explode_cols, select_cols):
    """
    Abstract creation of DF into a function
    :param schema: schema provided for the dataframe
    :param file: file that needs to be turned into a df
    :param explode_cols: tuple of column name, column
    :param select_cols: list of columns to select
    :return: dataframe
    """
    rdd = sftp_json_to_rdd(spark_session, file)
    base_df = (spark_session
                .read
                .schema(schema)
                .option("encoding", "ascii")
                .option("timestampFormat", "yyyy-MM-dd-HH-mm-ss")
                .option("dateFormat", "yyyy-MM-dd")
                .json(rdd))
    for name, column in explode_cols:
        base_df = base_df.withColumn(name, F.explode(column))

    return base_df.select(*select_cols)


def create_advertiser_df_from_sftp_file(files):
    '''
    This function:
    1) reads in an advertiser file from an efs-mount to the mediaocean-SFTP server
    2) flattens the all the nested json structs and elements

    :param spark: spark session
    :param filepath: pythonic-efs path to the json file
    :return: a flattened dataframe of the advertisers json file
    '''
    advertisers_schema = StructType([
        StructField('moSystem', StringType(), True)
        , StructField("tenant", StringType(), True)
        , StructField("system", StringType(), True)
        , StructField("advertiserMappedByMedia", StringType(), True)
        , StructField("feedType", StringType(), True)
        , StructField("filename", StringType(), True)
        , StructField('extractdatetime', TimestampType(), True)
        , StructField('advertisers', ArrayType(
            StructType([
                StructField("id", StringType(), True)
                , StructField("name", StringType(), True)
                , StructField("media", StringType(), True)
                , StructField("brands", ArrayType(
                    StructType([
                        StructField("id", StringType(), True)
                        , StructField("name", StringType())
                    ])
                ))
            ])
        ))
    ])

    explode_cols = [("advertisers", F.col("advertisers")), ("brands", F.col("advertisers.brands"))]
    select_cols = [
        F.col("moSystem").alias("mo_system"),
        F.col("tenant"),
        F.col("system"),
        F.col("advertiserMappedByMedia").alias("advertiser_mapped_by_media"),
        F.col("feedType").alias("feed_type"),
        F.col("filename"),
        F.col("extractdatetime").alias("extract_datetime"),
        F.col("advertisers.id").alias("advertiser_id"),
        F.col("advertisers.name").alias("advertiser_name"),
        F.col("advertisers.media").alias("advertiser_media"),
        F.col("brands.id").alias("brand_id"),
        F.col("brands.name").alias("brand_name")
    ]

    return functools.reduce(DataFrame.unionAll, list(map(lambda f: create_df(advertisers_schema, f, explode_cols, select_cols), files)))


def create_estimate_df_from_sftp_file(files):
    '''
    :param spark: the spark session
    :param filepath: pythonic-efs path to the json file
    :return:
    '''
    # the order of columns and column names for estimate-sftp files is different in 2017 compared to 2018 and 2019.
    # the column types are still the same
    # temporarily go schema free
    # kenny has a way an elegant way of handling data files where the column changes frequently in raw-dcm data files
    # 'brandId' field in was renamed to 'brandIds' in 2018 onwards
    estimate_schema_2017 = StructType([
        StructField("estimates", ArrayType(
            StructType([
                StructField("advertiserId", StringType(), True)
                , StructField("brandId", StringType(), True)
                , StructField("endDate", DateType(), True)
                , StructField("id", StringType(), True)
                , StructField("media", StringType(), True)
                , StructField("name", StringType(), True)
                , StructField("startDate", DateType(), True)
            ])
        ))
        , StructField("extractdatetime", TimestampType(), True)
        , StructField("feedType", StringType(), True)
        , StructField("filename", StringType(), True)
        , StructField("moSystem", StringType(), True)
        , StructField("system", StringType(), True)
        , StructField("tenant", StringType(), True)
    ])

    estimate_schema_2018_onwards = StructType([
        StructField("moSystem", StringType(), True)
        , StructField("tenant", StringType(), True)
        , StructField("system", StringType(), True)
        , StructField("feedType", StringType(), True)
        , StructField("filename", StringType(), True)
        , StructField("extractdatetime", TimestampType(), True)
        , StructField("estimates", ArrayType(
            StructType([
                StructField("id", StringType(), True)
                , StructField("name", StringType(), True)
                , StructField("startDate", DateType(), True)
                , StructField("endDate", DateType(), True)
                , StructField("media", StringType(), True)
                , StructField("advertiserId", StringType(), True)
                , StructField("brandIds", ArrayType(StringType()), True)
            ])
        ))
    ])

    explode_cols = [("estimates", F.col("estimates"))]
    select_cols = [
        F.col("moSystem").alias("mo_system"),
        F.col("tenant"),
        F.col("system"),
        F.col("feedType").alias("feed_type"),
        F.col("filename"),
        F.col("extractdatetime").alias("extract_datetime"),
        F.col("estimates.id").alias("id"),
        F.col("estimates.name").alias("estimate_name"),
        F.col("estimates.startDate").cast(DateType()).alias("start_date"),
        F.col("estimates.endDate").cast(DateType()).alias("end_date"),
        F.col("estimates.media").alias("media"),
        F.col("estimates.advertiserId").alias("advertiser_id"),
        F.col("estimates.brandId").alias("brand_id")
    ]

    return functools.reduce(DataFrame.unionAll, list(map(lambda f: create_df(estimate_schema_2017, f, explode_cols, select_cols), files)))


def create_daypart_df_from_sftp_file(files):
    '''
    :param spark:
    :param filepath:
    :return:
    '''
    daypart_schema = StructType([
        StructField("moSystem", StringType(), True)
        , StructField("tenant", StringType(), True)
        , StructField("system", StringType(), True)
        , StructField("feedType", StringType(), True)
        , StructField("filename", StringType(), True)
        , StructField("extractdatetime", TimestampType(), True)
        , StructField("dayparts", ArrayType(
            StructType([
                StructField("id", StringType(), True)
                , StructField("name", StringType(), True)
                , StructField("media", StringType(), True)
                , StructField("advertiserId", StringType(), True)
            ])
        ), True)
    ])

    explode_cols = [("dayparts", F.col("dayparts"))]
    select_cols = [
        F.col("moSystem").alias("mo_system"),
        F.col("tenant"),
        F.col("system"),
        F.col("feedType").alias("feed_type"),
        F.col("filename"),
        F.col("extractdatetime").alias("extract_datetime"),
        F.col("dayparts.id").alias("id"),
        F.col("dayparts.name").alias("daypart_name"),
        F.col("dayparts.media").alias("media"),
        F.col("dayparts.advertiserId").alias("advertiser_id")  # advertiser_id can be NULL a lot of times
    ]

    return functools.reduce(DataFrame.unionAll, list(map(lambda f: create_df(daypart_schema, f, explode_cols, select_cols), files)))


def create_destination_df_from_sftp_file(files):
    '''
    :param spark:
    :param filepath:
    :return:
    '''
    destination_schema = StructType([
        StructField("moSystem", StringType(), True)
        , StructField("tenant", StringType(), True)
        , StructField("system", StringType(), True)
        , StructField("feedType", StringType(), True)
        , StructField("filename", StringType(), True)
        , StructField("extractdatetime", TimestampType(), True)
        , StructField("destinations", ArrayType(
            StructType([
                StructField("id", StringType(), True)
                , StructField("name", StringType(), True)
                , StructField("media", StringType(), True)
                , StructField("type", StringType(), True)
                , StructField("nielsenCode", StringType(), True)
            ])
        ), True)
    ])

    explode_cols = [("destinations", F.col("destinations"))]
    select_cols = [
        F.col("moSystem").alias("mo_system"),
        F.col("tenant"),
        F.col("system"),
        F.col("feedType").alias("feed_type"),
        F.col("filename"),
        F.col("extractdatetime").alias("extract_datetime"),
        F.col("destinations.id").alias("id"),
        F.col("destinations.name").alias("name"),
        F.col("destinations.media").alias("media"),
        F.col("destinations.nielsenCode").alias("nielsen_code"),
        F.col("destinations.type").alias("type")
    ]

    return functools.reduce(DataFrame.unionAll, list(map(lambda f: create_df(destination_schema, f, explode_cols, select_cols), files)))


def create_program_df_from_sftp_file(files):
    '''

    :param spark:
    :param filepath:
    :return:
    '''
    program_schema = StructType([
        StructField("moSystem", StringType(), True)
        , StructField("tenant", StringType(), True)
        , StructField("system", StringType(), True)
        , StructField("feedType", StringType(), True)
        , StructField("filename", StringType(), True)
        , StructField("extractdatetime", TimestampType(), True)
        , StructField("programs", ArrayType(
            StructType([
                StructField("id", StringType(), True)
                , StructField("name", StringType(), True)
                , StructField("networkId", StringType(), True)
                , StructField("startDate", DateType(), True)
                , StructField("endDate", DateType(), True)
                , StructField("media", StringType(), True)
            ])
        ), True)
    ])

    explode_cols = [("programs", F.col("programs"))]
    select_cols = [
        F.col("moSystem").alias("mo_system"),
        F.col("tenant"),
        F.col("system"),
        F.col("feedType").alias("feed_type"),
        F.col("filename"),
        F.col("extractdatetime").alias("extract_datetime"),
        F.col("programs.id").alias("program_id"),
        F.col("programs.name").alias("program_name"),
        F.col("programs.networkId").alias("program_network_id"),
        F.col("programs.endDate").alias("program_end_date"),
        F.col("programs.media").alias("program_start_date"),
        F.col("programs.media").alias("program_media")
    ]
    return functools.reduce(DataFrame.unionAll, list(map(lambda f: create_df(program_schema, f, explode_cols, select_cols), files)))


def write_df_to_postgres(df, jdbc_table, num_partitions=20):
    '''
    This function writes a spark_df to a postgres table

    :param df: spark-df to write
    :param postgres_props: python-dict containing the postgres connection params
    :param jdbc_url: url to the postgres database
    :param jdbc_table: postgres table name to write the spark-df
    :param num_partitions: parallel writes
    :return: None
    '''
    print(f"writing to {jdbc_table}")
    (df
     .repartition(num_partitions)
     .write
     .option("numPartitions", num_partitions)
     .option("stringtype", "unspecified")
     .jdbc(url=url, table=jdbc_table, mode="append", properties=properties))


def read_df_from_postgres(jdbc_table, num_partitions=1):
    return spark_session.read.jdbc(url=url, properties=properties, table=jdbc_table, numPartitions=num_partitions)


if __name__ == "__main__":
    feed_map = {
        "advertiser": "ADVERTISERFEED",
        "destination": "DESTINATIONFEED",
        "estimate": "ESTIMATEFEED",
        "daypart": "DAYPARTFEED",
        "program": "PROGRAMFEED"
    }
    efs_path = '/efs-mediaocean/mediaocean'

    parser = argparse.ArgumentParser(description="Get necessary parameters for Mediaocean ingest.")
    parser.add_argument("--feed", type=str, help="type of feed")
    options = parser.parse_args()

    type_of_feed = feed_map.get(options.feed, '')

    if type_of_feed:
        files_to_ingest = os.listdir(efs_path)
        files_to_ingest_final = list(map(lambda f: f"{efs_path}/{f}", files_to_ingest))
        run_ingest(files_to_ingest_final, type_of_feed)
    else:
        print("Enter invalid feed: [advertiser, destination, estimate, daypart, program]")
