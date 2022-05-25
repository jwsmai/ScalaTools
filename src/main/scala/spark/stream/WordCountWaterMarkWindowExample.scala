package spark.stream

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, element_at, max, split, window}

object WordCountWaterMarkWindowExample extends App{
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StructuredStreamingWordCountExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")

  // 设置需要监听的本机地址与端口号
  val host: String = "127.0.0.1"
  val port: String = "9999"

  //  nc -lk 9999

  // 从监听地址创建DataFrame
  var df: DataFrame = spark.readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load()


  /**
   * 使用DataFrame API完成Word Count计算
   * */
  // 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
  val countDf = df.withColumn("inputs", split(df("value"), ","))
    .withColumn("event_time", element_at(col("inputs"), 1).cast("timestamp"))
    .withColumn("word", element_at(col("inputs"), 2))
    // 根据event_time计算watermark
    .withWatermark("event_time", "10 minutes")
    // 10 mins windows, sliding every 5mins
    .groupBy(window(col("event_time"), "10 minutes", "5 minutes"), col("word"))
    .count()

  countDf.writeStream
    .format("console") // 指定Sink为终端（Console）
    .option("truncate", false) // 指定输出选项,“truncate”选项，用来表明输出内容是否需要截断。
//     .outputMode("complete")
    .outputMode("update")
    .start() // 启动流处理应用
    .awaitTermination() // 等待中断指令

}
