from pyspark import SparkConf
from pyspark.sql import SparkSession

HOST = "192.168.1.7"
PORT = "9000"


class IOManager:
    def __init__(self, spark_session):
        self.spark = spark_session

    def hdfs_save_dataframe_parquet(self, dataframe, file_name):
        path = f"hdfs://{HOST}:{PORT}/{file_name}.parquet"
        dataframe.write.mode("overwrite").parquet(path)


    def hdfs_load_dataframe_parquet(self, file_name):
        path = f"hdfs://{HOST}:{PORT}/{file_name}.parquet"
        return self.spark.read.parquet(path)


    def hdfs_save_dataframe_sql(self, dataframe, table_name):
        url = f"jdbc:postgresql://{HOST}:5432/spark-rw"
        dataframe.write.format("jdbc")\
            .option("url", url)\
            .option("dbtable", table_name)\
            .option("user", "spark-rw")\
            .save(mode="overwrite")
