from pyspark import SparkConf
from pyspark.sql import SparkSession

from input_output_manager import IOManager

spark = SparkSession.builder.master("spark://192.168.1.2:7077")\
        .config('spark.driver.extraClassPath', 'jdbc_driver/postgresql-42.2.14.jar')\
        .getOrCreate()

def transform_text_data_sql():
    io_manager = IOManager(spark)

    path_object = "data_sample/object"
    objects = spark.read.json(path_object) # can be path or single file
    # objects.printSchema()
    # objects.show(2)

    path_image = "data_sample/image"
    images = spark.read.json(path_image)
    # images.printSchema()
    # images.show(2)

    io_manager.hdfs_save_dataframe_sql(images, 'images')
    io_manager.hdfs_save_dataframe_sql(objects, 'objects')

transform_text_data_sql()
