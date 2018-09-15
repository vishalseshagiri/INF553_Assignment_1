from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as func
from pyspark.sql.functions import col

if __name__=="__main__":

    ratings_input_path = sys.argv[1]
    tags_input_path = sys.argv[2]
    output_path = sys.argv[3]
    spark = SparkSession \
            .builder \
            .appName("Assignment 1 Task 2") \
            .getOrCreate()

    ratings_data = spark \
                    .read \
                    .format("csv") \
                    .option("header", "true") \
                    .load("{}".format(ratings_input_path), inferSchema=True)

    tags_data = spark \
                    .read \
                    .format("csv") \
                    .option("header", "true") \
                    .load("{}".format(tags_input_path), inferSchema=True)

    join_data = tags_data.join(ratings_data, on=['movieId'], how="inner")

    join_data = join_data['tag', 'rating']

    final_result = join_data.groupBy("tag").agg(func.avg("rating").alias("rating_avg"))

    final_result \
        .sort(col("tag").desc()) \
        .coalesce(1) \
        .write \
        .format("csv") \
        .save("{}".format(output_path), header=True)


