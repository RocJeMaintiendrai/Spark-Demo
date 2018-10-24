package main.scala.com.spark.sql.logstats

import main.scala.com.Util.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer


/**
  * @author PGOne
  * @ date 2018/10/24
  *      打印日志的一些统计信息
  */
object LogStats {


  /**
    * 求每个域名的流量
    * @param spark
    * @param inputFile
    */
  def printTrafficByDomain(spark: SparkSession, inputFile: String) = {

    spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(LogConverter.structType)
      .load(inputFile)
      .groupBy(LogConverter.C_DOMAIN)
      .agg(sum(LogConverter.C_TRAFFIC).as("total_traffic"))
      .sort(desc("total_traffic"))
      .select(LogConverter.C_DOMAIN, "total_traffic")
      .show(false)

  }

  /**
    * 求省份的访问次数(降序)
    *
    * @param spark
    * @param inputFile
    */
  def printPVByProvince(spark: SparkSession, inputFile: String) = {
    val logDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(LogConverter.structType)
      .load(inputFile)

    import spark.implicits._
    logDF.mapPartitions(logs => {
      val result = ArrayBuffer[(String, Int)]()
      while(logs.hasNext) {
        val log = logs.next()
        result += ((Utils.getProvince(log(LogConverter.IDX_IP).asInstanceOf[String]), 1))
      }
      result.iterator
    })
      .groupBy($"_1".as("province"))
      .count()
      .sort(desc("count"))
      .select("province", "count")
      .show(false)
  }



  /**
    * 求每个域名下访问数最多的文件资源 Top3
    * 文件资源(需要干掉url参数和域名)
    *
    * @param spark
    * @param inputFile
    */
  def printMaxPVResourceByDomain(spark: SparkSession, inputFile: String) = {
    val logDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(LogConverter.structType)
      .load(inputFile)

    import spark.implicits._
    val logStatsDF = logDF.mapPartitions(logs => {
      val result = ArrayBuffer[(String, String, Int)]()
      while(logs.hasNext) {
        val log = logs.next()
        result += (
          (log(LogConverter.IDX_DOMAIN).toString,
            Utils.getUrlIgnoreParam(log(LogConverter.IDX_URL).asInstanceOf[String]),
            1)
          )
      }
      result.iterator
    }).groupBy($"_1".as("domain"), $"_2".as("resource"))
      .count()

    logStatsDF.select(
      logStatsDF("domain"),
      logStatsDF("resource"),
      logStatsDF("count"),
      row_number()
        .over(Window.partitionBy(logStatsDF("domain"))
          .orderBy(logStatsDF("count").desc))
        .as("count_rank")
    )
      .filter("count_rank <= 3")
      .show(false)

  }


}
