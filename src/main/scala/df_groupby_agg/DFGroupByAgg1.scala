package df_groupby_agg

/**
  * groupBy分组和使用agg聚合函数demo
  */
object DFGroupByAgg1 {

  def main(args: Array[String]): Unit = {
    /**
      * 假设有下面这样的一个df
      * +----+-----+---+
      * |YEAR|MONTH|NUM|
      * +----+-----+---+
      * |2017|    1| 10|
      * |2017|    1|  5|
      * |2017|    2| 20|
      * |2018|    1|  5|
      * |2018|    1|  5|
      * +----+-----+---+
      */

    import org.apache.spark.sql.functions._
    df.groupBy("YEAR", "MONTH")
      .agg(sum("NUM").as("sum_num"))
      .show
    /**
      * +----+-----+-------+
      * |YEAR|MONTH|sum_num|
      * +----+-----+-------+
      * |2018|    1|   10.0|
      * |2017|    1|   15.0|
      * |2017|    2|   20.0|
      * +----+-----+-------+
      */

    // 也可以这样写
    df.groupBy("YEAR", "MONTH")
      .agg("NUM"->"avg", ("MONTH", "count") )
      .show
    /**
      * +----+-----+--------+------------+
      * |YEAR|MONTH|avg(NUM)|count(MONTH)|
      * +----+-----+--------+------------+
      * |2018|    1|     5.0|           2|
      * |2017|    1|     7.5|           2|
      * |2017|    2|    20.0|           1|
      * +----+-----+--------+------------+
      */
  }

}
