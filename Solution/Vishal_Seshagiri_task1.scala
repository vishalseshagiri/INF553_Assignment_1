import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, avg, _}

object Task1 {
  def main(arg:Array[String]):Unit = {

    val output_path = arg(1)
    val input_path = arg(0)
    val spark = SparkSession.builder.master("local[2]").appName("Assignment 1 Task 1").getOrCreate()
    val ratings_data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      //      .load("../../data/ml-latest-small/ratings.csv")
      .load(input_path)

    var required_data = ratings_data.select("movieId", "rating")

    required_data = required_data.groupBy("movieId").agg(avg("rating").alias("rating_avg"))

    required_data.sort(asc("movieId"))
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(output_path)

  }
}
