package df_group_sort_topn
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * 使用dataframe解决spark TopN问题：分组、排序、取TopN
  * https://www.itdaan.com/blog/2017/11/21/e95aaad884ce15ed802ba5e76a1cc55f.html
  */
object DFGroupSortTopn {


    def main(args: Array[String]): Unit = {
      val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DFGroupSortTopn")
      val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
      val sc = sparkSession.sparkContext

//      val sqlContext = new SQLContext(sc)
      import sparkSession.implicits._

//      1、使用dataframe解决spark TopN问题

      val df = sc.parallelize(Seq(
        (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
        (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
        (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
        (3,"cat8",35.6))).toDF("Hour", "Category", "TotalValue")

      df.show
      /*
      +----+--------+----------+
      |Hour|Category|TotalValue|
      +----+--------+----------+
      |   0|   cat26|      30.9|
      |   0|   cat13|      22.1|
      |   0|   cat95|      19.6|
      |   0|  cat105|       1.3|
      |   1|   cat67|      28.5|
      |   1|    cat4|      26.8|
      |   1|   cat13|      12.6|
      |   1|   cat23|       5.3|
      |   2|   cat56|      39.6|
      |   2|   cat40|      29.7|
      |   2|  cat187|      27.9|
      |   2|   cat68|       9.8|
      |   3|    cat8|      35.6|
      +----+--------+----------+
      */

//      val w = Window.partitionBy($"Hour").orderBy($"TotalValue".desc)
      // $有些版本可以省略，有些版本采用col()
      // $"TotalValue".desc，在有些版本写成 desc("TotalValue")
      val w = Window.partitionBy($"Hour").orderBy(desc("TotalValue"))
      // 取Top1
      val dfTop1 = df.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
      // 注意：row_number()在spark1.x版本中为rowNumber(),在2.x版本为row_number()
      // 取Top3
      val dfTop3 = df.withColumn("rn", row_number.over(w)).where($"rn" <= 3).drop("rn")

      dfTop1.show
      /*
      +----+--------+----------+
      |Hour|Category|TotalValue|
      +----+--------+----------+
      |   1|   cat67|      28.5|
      |   3|    cat8|      35.6|
      |   2|   cat56|      39.6|
      |   0|   cat26|      30.9|
      +----+--------+----------+
      */

//      dfTop3.show
      /*
      +----+--------+----------+
      |Hour|Category|TotalValue|
      +----+--------+----------+
      |   1|   cat67|      28.5|
      |   1|    cat4|      26.8|
      |   1|   cat13|      12.6|
      |   3|    cat8|      35.6|
      |   2|   cat56|      39.6|
      |   2|   cat40|      29.7|
      |   2|  cat187|      27.9|
      |   0|   cat26|      30.9|
      |   0|   cat13|      22.1|
      |   0|   cat95|      19.6|
      +----+--------+----------+
      */

    }

}
