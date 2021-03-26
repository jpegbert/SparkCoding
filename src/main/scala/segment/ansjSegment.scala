package segment

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}


object ansjSegment {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点

    val rdd = sc.textFile("data.txt")
      .map { x =>
        var str = if (x.length > 0)
          ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" ")
        str.toString
      }.top(50).foreach(println)
  }

}
