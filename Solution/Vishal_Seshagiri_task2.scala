import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, avg, _}

object Task2 {

  def main(arg:Array[String]):Unit = {

    val spark = SparkSession.builder.master("local[2]").appName("Assignment 1 Task 2").getOrCreate()

    val ratings_input = arg(0)
    val tags_input = arg(1)
    val output_path = arg(2)

    val ratings_data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(ratings_input)
      .as("ratings_data")

    val tags_data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(tags_input)
      .as("tags_data")

    val join_data = tags_data.join(ratings_data,
      col("tags_data.movieId") === col("ratings_data.movieId"),
      "inner")

    val final_result = join_data.groupBy("tag") .agg(avg("rating").alias("rating_avg"))

    final_result.sort(desc("tag"))
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(output_path)

  }
}
