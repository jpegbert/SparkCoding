package com.rdd_2_df

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 总结RDD转换成DF的三种方法
  */
object Rdd2DF {

  case class People(var name:String ,var age : Int)

  /**
    * RDD转换成DF的第一种转换方法
    */
  def transfer1(): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd2DF")
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
    val schema = StructType(
      Seq(
        StructField("name",StringType,true)
        ,StructField("age",IntegerType,true)
      )
    )
    val rowRDD = sparkSession.sparkContext
      .textFile("D:/people.txt",2)
      .map( x => x.split(",")).map( x => Row(x(0), x(1).trim().toInt))
    sparkSession.createDataFrame(rowRDD, schema)
  }

  /**
    * RDD转换成DF的第二种转换方法
    */
  def transfer2(): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd2DF")
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
    //导入隐饰操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val peopleRDD = sparkSession.sparkContext
      .textFile("/tmp/people.txt",2)
      .map( x => x.split(",")).map( x => People(x(0), x(1).trim().toInt))
    val peopleDF = peopleRDD.toDF()
  }


  /**
    * RDD转换成DF的第三种转换方法
    */
  def transfer3(): Unit = {
    // 初始化 spark config
    val conf = new SparkConf().setAppName("Rdd2DF").setMaster("local")
    // 初始化spark context
    val sc = new SparkContext(conf)
    // 初始化spark sql context
    val ssc = new SQLContext(sc)

    // 做spark sql 的df获取工作
    val peopleRDD = sc.textFile("D:/people.txt").map(line =>
      Row(line.split(" ")(0), line.split(" ")(1).trim().toInt))
    // 创建 StructType 来定义结构
    val structType : StructType = StructType(
      StructField("name",StringType,true)::
      StructField("age",IntegerType,true) ::Nil
    )
    val df : DataFrame = ssc.createDataFrame(peopleRDD, structType)
    /*df.registerTempTable("peopel")
    ssc.sql("select * from peopel").show()
    sc.stop()*/
  }

  /**
    * RDD转换成DF的第四种转换方法
    */
  def transfer4(): Unit = {
    // 初始化 spark config
    val conf = new SparkConf().setAppName("Rdd2DF").setMaster("local")
    // 初始化spark context
    val sc = new SparkContext(conf)
    // 初始化spark sql context
    val ssc = new SQLContext(sc)
    // 做spark sql 的df获取工作
    val PeopleRDD = sc.textFile("D:/people.txt").map(line => People(line.split(" ")(0), line.split(" ")(1).trim.toInt))

    import ssc.implicits._

    val df = PeopleRDD.toDF

    /*// 将DataFrame注册成临时的一张表，这张表相当于临时注册到内存中，是逻辑上的表，不会物化到磁盘  这种方式用的比较多
    df.registerTempTable("peopel")
    val df2 = ssc.sql("select * from peopel where age > 23")show()
    sc.stop()*/
  }

  /**
    * RDD转换成DF的第五种转换方法
    */
  def transfer5(): Unit = {
    // 初始化 spark config
    val conf = new SparkConf().setAppName("Rdd2DF").setMaster("local")
    // 初始化spark context
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    // 做spark sql 的df获取工作
    val PeopleRDD = sc.textFile("D:/people.txt").map(line => People(line.split(" ")(0), line.split(" ")(1).trim.toInt))
    sqlContext.createDataFrame(PeopleRDD)
  }

  def main(args: Array[String]): Unit = {

  }

}
