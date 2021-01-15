package merge_df_cols

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StringType


/**
  * DataFrame 合并列
  *
  * https://www.cnblogs.com/itboys/p/9813934.html
  */
object MergeDFCols {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    // 从内存创建一组DataFrame数据
    import spark.implicits._
    val df = Seq(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L))
      .toDF("name", "age", "phone")
    df.show()
    /**
      * +----+---+-----------+
      * |name|age|      phone|
      * +----+---+-----------+
      * |Ming| 20|15552211521|
      * |hong| 19|13287994007|
      * | zhi| 21|15552211523|
      * +----+---+-----------+
      */

    // 方法1：利用map重写
    val separator = ","
    val result = df.map(_.toSeq.foldLeft("")(_ + separator + _).substring(1))
    /**
      * +-------------------+
      * |              value|
      * +-------------------+
      * |Ming,20,15552211521|
      * |hong,19,13287994007|
      * | zhi,21,15552211523|
      * +-------------------+
      */

    // 方法2：使用内置函数 concat_ws
    import org.apache.spark.sql.functions._
    val res = df.select(concat_ws(separator, $"name", $"age", $"phone").cast(StringType).as("value")).show()
    /**
      * +-------------------+
      * |              value|
      * +-------------------+
      * |Ming,20,15552211521|
      * |hong,19,13287994007|
      * | zhi,21,15552211523|
      * +-------------------+
      */

    //方法3：使用自定义UDF函数
    // 编写udf函数
    def mergeCols(row: Row): String = {
      row.toSeq.foldLeft("")(_ + separator + _).substring(1)
    }

    val mergeColsUDF = udf(mergeCols _)
    df.select(mergeColsUDF(struct($"name", $"age", $"phone")).as("value")).show()
    /**
      * /**
      * * +-------------------+
      * * |              value|
      * * +-------------------+
      * * |Ming,20,15552211521|
      * * |hong,19,13287994007|
      * * | zhi,21,15552211523|
      * * +-------------------+
      **/
      */
  }

}
