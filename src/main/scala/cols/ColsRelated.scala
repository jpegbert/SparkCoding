package cols

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}


// https://blog.csdn.net/an1090239782/article/details/102541230

object ColsRelated {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val idCol1 = $"id"
    val idCol2 = col("id")
    val idCol3 = column("id")


    val dataset = spark.range(5).toDF("text")
    val textCol1 = dataset.col("text")
    val textCol2 = dataset.apply("text")
    val textCol3 = dataset("text")
  }

}
