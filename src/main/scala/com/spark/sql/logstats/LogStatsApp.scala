package main.scala.com.spark.sql.logstats
import org.apache.spark.sql.SparkSession
/**
  * @author PGOne
  * @ date 2018/10/24
  *   * 0904需求：使用DF/DS统计日志
  *          1.打印每个域名的流量
  *          2.打印省份的访问次数(降序)
  *          3.打印每个域名下访问最多的文件资源top3
  *

  */
object LogStatsApp {
  object LogStatsApp {
    def main(args: Array[String]): Unit = {
      val inputFile = "file:///Users/bullfly/IdeaProjects/demo-test/data/access-201707.log"

      val spark = SparkSession.builder().appName("LogStatsApp")
        .master("local[2]").getOrCreate()

      //求每个域名的流量
      LogStats.printTrafficByDomain(spark, inputFile)

      //求省份的访问次数(降序)
      LogStats.printPVByProvince(spark, inputFile)

      //求每个域名下访问数最多的文件资源TopN
      LogStats.printMaxPVResourceByDomain(spark, inputFile)

      spark.stop
    }
  }


}
