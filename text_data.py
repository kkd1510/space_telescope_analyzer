from pyspark import SparkConf
from pyspark.sql import SparkSession

from input_output_manager import IOManager

IMAGE_T = 'image'
OBJECT_T = 'object'

PERSIST = True

def transform_text_data_sql(data_type):

    path_object = f"data_sample/{data_type}"
    read_json_df = spark.read.json(path_object)
    read_json_df.printSchema()
    read_json_df.show(2)

    if PERSIST:
        io_manager = IOManager(spark)
        io_manager.hdfs_save_dataframe_sql(read_json_df, 'images')

spark = SparkSession.builder.master("local")\
        .config('spark.driver.extraClassPath', 'jdbc_driver/postgresql-42.2.14.jar')\
        .getOrCreate()

transform_text_data_sql(IMAGE_T)
transform_text_data_sql(OBJECT_T)
