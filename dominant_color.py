import cv2
import numpy as np
from pyspark.sql import SparkSession
from collections import Counter

import glob

from pyspark.context import SparkContext
from sklearn.cluster import KMeans


def dominant_color(image_path, kmeans_clusters=3):
    img = cv2.imread(image_path)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    img = img.reshape((img.shape[0] * img.shape[1], 3))
    kmeans = KMeans(n_clusters=kmeans_clusters, random_state=1)
    kmeans.fit(img)
    return tuple(map(tuple, kmeans.cluster_centers_.astype(int)))


spark = SparkSession.builder \
    .master("local") \
    .appName("test") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
images = glob.glob("images/*.jpg")
#images = [1,2,3,4,5,6,7,8]
rdd = spark.sparkContext.parallelize(images).map(dominant_color)
print(rdd.take(2))
# df = spark.createDataFrame(images, ['image',
#                                     'color_1',
#                                     'color_2',
#                                     'color_3',
#                                     'color_4',
#                                     'color_5'])
# df = spark.read.format("image").option("dropInvalid", True).load("images")
# df.select("image.origin", "image.width", "image.height").show(truncate=False)
# print(type(df))

# resize
# find dominant color
# numpy to rdd
# save file
# fuzzy match and order results based on distance
# show results

# metadata scrape
# store to sql

# https://www.mdpi.com/2073-8994/11/8/963/htm