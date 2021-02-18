package rdd_group_sort_topn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * 使用RDD解决spark TopN问题：分组、排序、取TopN
  * https://www.itdaan.com/blog/2017/11/21/e95aaad884ce15ed802ba5e76a1cc55f.html
  */
object RddGroupSortTopn {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DFGroupSortTopn")
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
    val sc = sparkSession.sparkContext

    //      val sqlContext = new SQLContext(sc)
    import sparkSession.implicits._

    val rdd1 = sc.parallelize(Seq(
      (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
      (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
      (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
      (3,"cat8",35.6)))

    val rdd2 = rdd1.map(x => (x._1,(x._2, x._3))).groupByKey()
    /*
    rdd2.collect
    res9: Array[(Int, Iterable[(String, Double)])] = Array((0,CompactBuffer((cat26,30.9), (cat13,22.1), (cat95,19.6), (cat105,1.3))),
                                                           (1,CompactBuffer((cat67,28.5), (cat4,26.8), (cat13,12.6), (cat23,5.3))),
                                 (2,CompactBuffer((cat56,39.6), (cat40,29.7), (cat187,27.9), (cat68,9.8))),(3,CompactBuffer((cat8,35.6))))

    */
    val N_value = 1  // 取前3

    val rdd3 = rdd2.map( x => {
      val i2 = x._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > N_value) i2_2.remove(0, (i2_2.length - N_value))
      (x._1, i2_2.toIterable)
    })

    /*
     rdd3.collect
    res8: Array[(Int, Iterable[(String, Double)])] = Array((0,ArrayBuffer((cat95,19.6), (cat13,22.1), (cat26,30.9))),
                                                           (1,ArrayBuffer((cat13,12.6), (cat4,26.8), (cat67,28.5))),
                                 (2,ArrayBuffer((cat187,27.9), (cat40,29.7), (cat56,39.6))),(3,ArrayBuffer((cat8,35.6))))
    */

    val rdd4 = rdd3.flatMap(x => {
      val y = x._2
      for (w <- y) yield (x._1, w._1, w._2)
    })

    rdd4.collect
    /*
    res3: Array[(Int, String, Double)] = Array((0,cat95,19.6), (0,cat13,22.1), (0,cat26,30.9),
                                               (1,cat13,12.6), (1,cat4,26.8), (1,cat67,28.5),
                           (2,cat187,27.9), (2,cat40,29.7), (2,cat56,39.6), (3,cat8,35.6))
    */

    rdd4.toDF("Hour", "Category", "TotalValue").show
    /* +----+--------+----------+
     |Hour|Category|TotalValue|
     +----+--------+----------+
     |   0|   cat95|      19.6|
     |   0|   cat13|      22.1|
     |   0|   cat26|      30.9|
     |   2|  cat187|      27.9|
     |   2|   cat40|      29.7|
     |   2|   cat56|      39.6|
     |   1|   cat13|      12.6|
     |   1|    cat4|      26.8|
     |   1|   cat67|      28.5|
     |   3|    cat8|      35.6|
     +----+--------+----------+*/

  }

}
