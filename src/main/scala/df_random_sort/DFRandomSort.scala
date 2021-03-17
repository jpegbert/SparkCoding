package df_random_sort

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DFRandomSort {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    // 从内存创建一组DataFrame数据
    import spark.implicits._

    val df = spark.sparkContext.parallelize(Seq(
      (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
      (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
      (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
      (3,"cat8",35.6))).toDF("Hour", "Category", "TotalValue")

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

    // 方法1：按照行随机打乱顺序
    val df_rand = df.orderBy(rand())

    // 方法2：先加随机列，再按照随机列排序(这里实现了一种先按照某字段分组，然后在分组内按行打乱的方法)
    val tmpDF = df.withColumn("rand_num", rand())
    val w_floor = Window.partitionBy("Hour").orderBy(asc("rand_num"))
    val randomDF = tmpDF.withColumn("rand_index", rank.over(w_floor))

  }

}
