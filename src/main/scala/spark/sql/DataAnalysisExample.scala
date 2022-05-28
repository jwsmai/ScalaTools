package spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, bround, col, lower, sum, trim}
import org.apache.spark.storage.StorageLevel

object DataAnalysisExample extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("DataAnalysisExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  def transformColumns(csvPath: String): DataFrame = {
    val df: DataFrame = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(csvPath)

    df.printSchema()

    val AnalysisDf = df
      .select(
        trim(lower(col("订阅名称 (Subscription Name)"))).as("Subscription"),
        trim(lower(col("日期 (Date)"))).as("Date"),
        trim(lower(col("服务类型 (Meter Sub-Category)"))).as("SubCategory"),
        trim(lower(col("服务 (Meter Category)"))).as("Category"),
        trim(lower(col("服务资源 (Meter Name)"))).as("MeterName"),
        bround(col("扩展的成本 (ExtendedCost)"),2).as("ExtendedCost"),
        trim(lower(col("(Resource Group)"))).as("ResourceGroup")
      )
    AnalysisDf
  }

  def semiAndSum(df : DataFrame): DataFrame = {
    df.as("t1")
      .join(costDetails202205.as("t2"),
        col("t1.ResourceGroup") === col("t2.ResourceGroup")
          && col("t1.Category") === col("t2.Category"),
        "leftsemi")
      .groupBy(col("t1.Date"),col("t1.ResourceGroup"), col("t1.Category"), col("t1.MeterName"))
      .agg(sum("ExtendedCost").as("ExtendedCost"))
  }

  val costDetails202205 = transformColumns("src/main/resources/2022_5-cn.csv").persist(StorageLevel.MEMORY_ONLY)
  val costDetails202205plus = transformColumns("src/main/resources/Detail_2022_5.csv").persist(StorageLevel.MEMORY_ONLY)
  val costDetails202203 = transformColumns("src/main/resources/2022_3-cn.csv")
  val costDetails202204 = transformColumns("src/main/resources/2022_4-cn.csv")

  val costSum202203 = semiAndSum(costDetails202203).sort(col("ExtendedCost").desc)
  val costSum202204 = semiAndSum(costDetails202204).sort(col("ExtendedCost").desc)
  val costSum202205 = semiAndSum(costDetails202205plus).sort(col("ExtendedCost").desc)

//  costSum202203.show(false)
//  costSum202204.show(false)
  costSum202205.show(false)



  def uselessResource() = {
    costSum202204.filter(col("ResourceGroup") === "databricks-rg-cbdatabricks-itirgrcyl6c5u")
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/4月废弃Databricks费用")

    costSum202203.filter(col("ResourceGroup") === "databricks-rg-cbdatabricks-itirgrcyl6c5u")
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/3月废弃Databricks费用")
  }

//  uselessResource()

  /**
    * @Description: 分析4月成本
    * @return: org.apache.spark.sql.RelationalGroupedDataset
  **/
  def analysis202204() = {
    costSum202204.groupBy(col("t1.ResourceGroup"), col("t1.Category"), col("t1.MeterName"))
      .agg(sum("ExtendedCost").as("ExtendedCost")).limit(20).coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/4月成本明细")

    costSum202204.filter(col("ResourceGroup") === "bigdata")
      .filter(col("Category") === "azure data factory v2")
      .groupBy(col("Date"), col("Category"), col("MeterName"))
      .agg(bround(sum(col("ExtendedCost")),2).as("ExtendedCost"))
      .filter(col("ExtendedCost") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/4月成本DWBI-ADF按天统计")

    costSum202204.filter(col("ResourceGroup") === "retailinfra")
      .filter(col("Category") === "azure databricks")
      .filter(col("MeterName") === "standard all-purpose compute dbu")
      .groupBy(col("Date"), col("Category"), col("MeterName"))
      .agg(bround(sum(col("ExtendedCost")),2).as("ExtendedCost"))
      .filter(col("ExtendedCost") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/4月成本DWBI-Databricks按天统计")

    costSum202204.filter(col("ResourceGroup") === "databricks-rg-dwbi-prod-rplfcyltqpqyu")
      .filter(col("Category") === "virtual machines")
      .groupBy(col("Date"), col("Category"), col("MeterName"))
      .agg(bround(sum(col("ExtendedCost")),2).as("ExtendedCost"))
      .filter(col("ExtendedCost") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/4月成本DWBI-virtual machines按天统计")
  }

//  analysis202204()

  def analysis202205() = {
    costSum202205.filter(col("ResourceGroup") === "retailinfra")
      .filter(col("Category") === "azure databricks")
      .filter(col("MeterName") === "standard all-purpose compute dbu")
      .groupBy(col("Date"), col("Category"), col("MeterName"))
      .agg(bround(sum(col("ExtendedCost")),2).as("ExtendedCost"))
      .filter(col("ExtendedCost") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/5月成本DWBI-Databricks按天统计")

    costSum202205.filter(col("ResourceGroup") === "databricks-rg-dwbi-prod-rplfcyltqpqyu")
      .filter(col("Category") === "virtual machines")
      .groupBy(col("Date"), col("Category"), col("MeterName"))
      .agg(bround(sum(col("ExtendedCost")),2).as("ExtendedCost"))
      .filter(col("ExtendedCost") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/5月成本DWBI-virtual machines按天统计")
  }

  analysis202205()

  /**
    * @Description: 比较4月和3月成本
    * @return: void
  **/
  def compare(): Unit = {

    val compareDetails = costSum202203.as("t1")
      .join(
        costSum202204.as("t2"),
        col("t1.ResourceGroup") === col("t2.ResourceGroup")
          && col("t1.Category") === col("t2.Category")
          && col("t1.MeterName") === col("t2.MeterName"),
        "inner"
      ).select(col("t1.ResourceGroup"),
      col("t1.Category"),
      col("t1.MeterName"),
      col("t1.ExtendedCost").as("202203cost"),
      col("t2.ExtendedCost").as("202204cost"),
      bround(col("t2.ExtendedCost") - col("t1.ExtendedCost"), 2).as("diff")
    )

    compareDetails
      .sort(col("diff").desc)
//      .filter(col("diff") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/按照按照资源组、Category、MeterName对比")

    compareDetails
      .groupBy(col("ResourceGroup"),col("Category"))
      .agg(sum("diff").as("diffSum"))
      .sort(col("diffSum").desc)
//      .filter(col("diffSum") > 0)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/按照资源组和Category对比")

    compareDetails.groupBy(col("ResourceGroup"))
      .agg(sum("diff").as("diffSum"))
      .sort(col("diffSum").desc)
      .coalesce(1)
      .write.option("header",true).mode(SaveMode.Overwrite)
      .csv("src/main/resources/按照资源组对比")
  }

//  compare()

}
