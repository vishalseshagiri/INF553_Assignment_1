from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as func

if __name__=="__main__":

    input_file_path = sys.argv[1]
    output_path = sys.argv[2]
    spark = SparkSession \
        .builder \
        .appName("Assignment 1 Task 1")\
        .getOrCreate()

    ratings_data = spark\
        .read\
        .format("csv")\
        .option("header", "true") \
        .load("{input_file_path}".format(input_file_path=input_file_path),\
        inferSchema=True)

    required_data = ratings_data['movieId', 'rating']

    required_data = required_data.groupBy("movieId").agg(func.avg("rating").alias("rating_avg"))

    required_data \
        .sort("movieId", ascending=True) \
        .coalesce(1) \
        .write \
        .format("csv") \
        .save("{output_path}".format(output_path=output_path), header=True)