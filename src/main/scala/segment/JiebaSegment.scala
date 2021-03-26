package segment

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.{SparkConf, SparkContext}

object JiebaSegment {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("data.txt")
      .map { x =>
        var str = if (x.length > 0)
          new JiebaSegmenter().sentenceProcess(x)
        str.toString
      }.top(50).foreach(println)
  }

}
