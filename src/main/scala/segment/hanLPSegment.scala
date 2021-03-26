package segment

import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.{SparkConf, SparkContext}

object hanLPSegment {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("data.txt")
      .map { x =>
        var str = if (x.length > 0)
          StandardTokenizer.segment(x)
        str.toString
      }.top(50).foreach(println)
  }

}
