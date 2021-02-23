package df_exploded

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * DataFrame一列爆炸成多行数据，并标记第一个数据是哪个
  * http://www.voidcn.com/article/p-uxjyuama-bwr.html
  */

object DFExploded {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DFGroupSortTopn")
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
    val sc = sparkSession.sparkContext

    // val sqlContext = new SQLContext(sc)
    import sparkSession.implicits._
    val df = sc.parallelize(Seq(
      (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
      (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
      (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
      (3,"cat8",35.6))).toDF("Hour", "Category", "TotalValue")
    // 把某一列爆炸开，行程多行
    var exploded_df = df.withColumn("exploded", explode(col("Category")))

    // 标记每一组数据的第一个是哪个，其实就是分组排序
    val window = Window.partitionBy("row_id").orderBy("row_id")
    // Create an internal rank variable to figure out the first element
    exploded_df = exploded_df.withColumn("_rank", row_number().over(window))
    exploded_df = exploded_df.withColumn("is_first",
      when((col("_rank") === 1), "Yes").otherwise("No"))


      // 也可以使用下面的方法
    import org.apache.spark.sql.functions._
    val rsultDF = df.select($"row_id", posexplode($"array_of_data"))
  }

}
