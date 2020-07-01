import cv2
import glob
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf

from sklearn.cluster import KMeans
from colormath.color_objects import LabColor
from colormath.color_diff import delta_e_cie2000
from colormath.color_objects import XYZColor, sRGBColor
from colormath.color_conversions import convert_color

from input_output_manager import IOManager

DATAFRAME_SCHEMA = ['image',
                    'color_1_r', 'color_1_g', 'color_1_b',
                    'color_2_r', 'color_2_g', 'color_2_b',
                    'color_3_r', 'color_3_g', 'color_3_b']

PARQUET_FILE_NAME = 'images_df'

REMOTE = True
spark = SparkSession.builder.master("spark://192.168.1.2:7077").config(conf=SparkConf()).getOrCreate()

def image_id(image_path):
    return os.path.basename(image_path).replace('_r.jpg', '').strip()


def find_dominant_colors(image_path, kmeans_clusters=3):
    img = cv2.imread(image_path)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    img = img.reshape((img.shape[0] * img.shape[1], 3))
    kmeans = KMeans(n_clusters=kmeans_clusters, random_state=1)
    kmeans.fit(img)

    attributes = [image_id(image_path)]
    for d_color in kmeans.cluster_centers_.astype(int):
        for rgb in d_color:
            attributes.append(rgb.item())
    return attributes


def assign_dominant_colors():
    images = glob.glob("images/*.jpg")

    images_dominant_color_rdd = spark.sparkContext.parallelize(images).map(find_dominant_colors)
    images_dataframe = images_dominant_color_rdd.toDF(DATAFRAME_SCHEMA)
    images_dataframe.show()
    io_manager = IOManager(spark)
    io_manager.hdfs_save_dataframe_parquet(images_dataframe, PARQUET_FILE_NAME)


def get_delta_e(r_1, g_1, b_1, r_2, g_2, b_2):
    rgb_param = sRGBColor(r_1, g_1, b_1)
    image_dominant_color = sRGBColor(r_2, g_2, b_2)

    color_param_xyz = convert_color(rgb_param, XYZColor)
    color_image_xyz = convert_color(image_dominant_color, XYZColor)

    color_param_lab = convert_color(color_param_xyz, LabColor)
    color_image_lab = convert_color(color_image_xyz, LabColor)

    return delta_e_cie2000(color_param_lab, color_image_lab)


def get_matching_images(desired_color):
    delta_e_udf = udf(lambda r, g, b: get_delta_e(r, g, b, *desired_color), FloatType())
    io_manager = IOManager(spark)

    images_df_parquet = io_manager.hdfs_load_dataframe_parquet(PARQUET_FILE_NAME)
    images_df_parquet.createOrReplaceTempView("images_df_parquet")
    full_images_df = spark.sql('select * from images_df_parquet')
    delta_e_assigned_df = full_images_df.select('image',
                                                delta_e_udf('color_1_r', 'color_1_g', 'color_1_b').alias(
                                                    'color_1_delta_e'),
                                                delta_e_udf('color_2_r', 'color_2_g', 'color_2_b').alias(
                                                    'color_2_delta_e'),
                                                delta_e_udf('color_3_r', 'color_3_g', 'color_3_b').alias(
                                                    'color_3_delta_e'))

    delta_e_assigned_df.show()

assign_dominant_colors()
