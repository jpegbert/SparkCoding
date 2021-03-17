package rdd_group_sort_topn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkStrategies
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object GroupByTopn {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("etpProcess")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._


    val rdd = sc.textFile("C:\\Users\\sss\\Desktop\\qqq\\aa.txt")
    val win = Window.partitionBy("id1").orderBy(desc("id3"))
    val rdd2 = rdd.map(line => {
      val data = line.split(",", -1)
      (data(0), data(1), data(2))
    })

    val data = rdd2.toDF("id1", "id2", "id3").withColumn("aa", lag("id3", 1).over(win))
    data.show()
    val df_difftime = data.withColumn("diff", when(isnull(col("id3") - col("aa")), 0)
      .otherwise((col("id3") - col("aa"))))
    df_difftime.show()

  }
}
