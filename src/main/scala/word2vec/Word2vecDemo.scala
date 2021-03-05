package word2vec

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext

/**
  * Spark word2vec demo
  */
object Word2vecDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2VecTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //    import sqlContext.implicits._

    val documentDF = sqlContext.createDataFrame(Seq("Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val word2Vec = new Word2Vec().
      setInputCol("text").
      setOutputCol("result").
      setVectorSize(3).
      setMinCount(0)

    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)

    val like = model.findSynonyms("Java", 5)
    like.foreach(println(_)) // 这种输出在日志中
    /*for((item, literacy) <- like){
      print(s"$item $literacy")
    }*/

  }

}
