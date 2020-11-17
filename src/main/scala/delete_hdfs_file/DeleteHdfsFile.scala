package delete_hdfs_file

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DeleteHdfsFile {

  /**
    * 删除hdfs文件目录方法1
    */
  def deleteHdfsPath1(sc: SparkContext, filePath: String): Unit = {
    val path = new Path(filePath)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      hdfs.delete(path, true)
    }
  }

  /**
    * 删除hdfs文件目录方法2
    */
  def deleteHdfsPath2(filePath: String, master: String = "hdfs://hadoopClusterStandbyService"): Unit = {
    val output = new org.apache.hadoop.fs.Path(master+"/user/jiangpeng.jiang/data/" + filePath)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(master), new org.apache.hadoop.conf.Configuration())
    // 删除目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder.appName("DeleteHdfsFile").master("yarn-client").getOrCreate
    val sc = spark.sparkContext*/
    val conf = new SparkConf().setAppName("DeleteHdfsFile")
    val sc = new SparkContext(conf)

    // 采用第一种方法删除
    deleteHdfsPath1(sc, "/user/data/tmp")
    // 采用第二种方法删除
    deleteHdfsPath2("/user/data/tmp")
  }

}
