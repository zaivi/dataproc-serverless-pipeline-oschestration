# import python library
import os
import time
import pandas as pd
import numpy as np

from vnstock import *

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list


# os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[*] pyspark-shell"
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-11.jdk"
# os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa/int-demo-looker-6910e0d96d67.json"



def create_spark_session() -> SparkSession:
    conf = SparkConf() \
            .set("spark.jars", "https://storage.googleapis.com/spark-lib/bigquery/spark-3.3-bigquery-0.32.2.jar") \
            .set("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")

    context = SparkContext(conf=conf)
    #Define custom context properties
    context.setCheckpointDir("checkpoints")

    spark_session = SparkSession\
        .builder \
        .config(conf=conf)\
        .appName("Spark to BigQuery VnStock ingestion") \
        .getOrCreate()

    return spark_session


def extract_cmp():
    cpm_lst = listing_companies()

    return cpm_lst


if __name__ == '__main__':

    spark = create_spark_session()

    cpm_df = extract_cmp()

    spark.conf.set("spark.sql.execution.arrow.enabled","true")
    sp_df=spark.createDataFrame(cpm_df) 
    sp_df.printSchema()
    sp_df.show()

    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector.
    bucket = "int-demo-looker-spark"
    spark.conf.set('temporaryGcsBucket', bucket)

    # Save the data to BigQuery
    sp_df.write.format('bigquery') \
    .option('table', 'int-demo-looker.spark_learning.stock_companies') \
    .mode("overwrite") \
    .save()

    # For UI to stick
    # time.sleep(1000000)
