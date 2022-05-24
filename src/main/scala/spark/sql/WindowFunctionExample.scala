package spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionExample extends App{

  val spark = SparkSession.builder()
    .appName("WindowFunctionExample")
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("error")
  import spark.implicits._

  case class Salary(depName: String, empNo: Long, salary: Long)

  val empsalary = Seq(
    Salary("sales", 1, 50),
    Salary("personnel", 2, 39),
    Salary("sales", 3, 48),
    Salary("personnel", 5, 35),
    Salary("develop", 7, 42),
    Salary("develop", 11, 52)).toDF()

  // 窗口函数
  val byDepName = Window.partitionBy("depName")
  empsalary.withColumn("depSalSum", sum("salary").over(byDepName))
    .withColumn("depSalMax", max("salary").over(byDepName))
    .withColumn("depSalMin", min("salary").over(byDepName))
    .withColumn("AllSum", sum("salary").over())
    .show()

}
