package spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array, col, collect_list, concat_ws, sort_array, struct}

object ConventStructTypeExample extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("DataAnalysisExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")

  import spark.implicits._

  val df: DataFrame = spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .csv("src/main/resources/textfile_3.csv")

  df.printSchema()
  df.show(10)

  val structDF = df.groupBy($"task_id", $"serial_id")
    .agg(
      collect_list(
        struct(
          struct(struct($"Xmin", $"Ymin").as("top_left"), struct($"Xmax", $"Ymax").as("bottom_right")).as("bounding_box"),
          struct($"sku_id", $"sku_name").as("sku_lists")
        )
      ).as("processed_results")
    ).dropDuplicates()

  structDF.printSchema()
  structDF.show(10, false)
  spark.stop()
}
