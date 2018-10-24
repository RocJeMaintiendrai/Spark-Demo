package main.scala.com.spark.core.log


import main.scala.com.Util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * @author PGOne
  * @ date 2018/10/24
  *  * 日志文件格式:
  * 域名  流量  资源  ip  访问时间
  *
  * www.zhibo8.com	3040	https://www.zhibo8.com/video/music/djembe.pdf	10.0.0.1	[2017-07-13 19:19:00]
  * www.dongqiudi.com	3940	https://www.dongqiudi.com/video/music/djembe.pdf	192.168.1.1	[2017-07-26 16:37:00]
  * www.youku.com	2160	https://www.youku.com/pdf/tech/go.pdf	172.30.0.1	[2017-07-05 13:49:00]
  * www.ruozedata.com	2770	https://www.ruozedata.com/video/tech/scala.mp4	192.168.1.1	[2017-07-13 11:36:00]
  * www.bilibili.com	2520	https://www.bilibili.com/pdf/tech/go.pdf	10.0.0.1	[2017-07-29 03:47:00]
  *

  */
class LogAnalyzer(fileName: String) {

  val sc = createLocalSparkContext
  val lines = sc.textFile(fileName)

  def createLocalSparkContext() = {
    val sparkConf = new SparkConf().setAppName(LogAnalyzer.APP_NAME).setMaster("local[2]")
    new SparkContext(sparkConf)
  }

  def stopSparkContext() = {
    sc.stop()
  }


  /**
    *
    *
    * 求每个域名的流量
    * 1. 获取1,2字段 => 域名，流量 | log => (1, 2)
    * 2. 按域名分组
    * 3. 分组后求和 reduceByKey(_+_)
    *
    */
  def printTrafficByDomain() = {
    //val lines = sc.textFile(fileName)
    //var arrayBuffer = ArrayBuffer()

    lines.map(x => {
      val record = x.split("\t")
      var traffic = 0L

      try {
        traffic = record(LogAnalyzer.TRAFFIC_IDX).trim.toLong
      } catch {
        case e: Exception => traffic = 0L
      }
      (record(0), traffic)

    }).reduceByKey(_+_).sortBy(_._2, false).foreach(println)
  }



  /**
    *
    *
    * 求省份的访问次数(降序)
    *
    * 1.获取 ip
    * 2.(省份,1)
    * 3.reduceByKey(_+_)  -> (北京市, 100)
    * 4.排序sortBy(_._2, false)
    *
    */
  def printPVByProvince() = {
    //val lines = sc.textFile(fileName)
    //var arrayBuffer = ArrayBuffer()

    lines.map(x => {
      val record = x.split("\t")
      (Utils.getProvince(record(LogAnalyzer.IP_IDX).trim), 1)
    }).reduceByKey(_+_)
      .sortBy(_._2, false)
      .take(40)
      .foreach(println)
  }

  /**
    * 求每个域名下访问数最多的文件资源
    *
    * 文件资源(需要干掉url参数和域名)
    *
    * https://www.youku.com/video/music/bass.mp4?key1=value1
    * /video/music/bass.mp4 <-- 资源文件定义
    *
    * ((域名, 资源), 1)  >> 域名＋资源 出现的次数
    * 域名内局部排序
    * groupBy(_._1._1)
    *
    * ((域名, 资源), 1)->groupBy(_._1._1)
    * (www.bilibili.com,CompactBuffer(((www.bilibili.com,/video/music/bass.mp4),1673), ((www.bilibili.com,/pdf/music/bass.pdf),1584), ((www.bilibili.com,),16523)))
    * (www.dongqiudi.com,CompactBuffer(((www.dongqiudi.com,/video/music/bass.mp4),1716), ((www.dongqiudi.com,),16736), ((www.dongqiudi.com,/pdf/music/bass.pdf),1671)))
    *
    */
  def printMaxPVResourceByDomain() = {
    //val lines = sc.textFile(fileName)
    //var arrayBuffer = ArrayBuffer()

    lines.map(x => {
      val record = x.split("\t")
      ((record(LogAnalyzer.DOMAIN_IDX).trim,
        Utils.getResource(record(LogAnalyzer.RESOURCE_IDX).trim)), 1)
    }).reduceByKey(_+_).groupBy(_._1._1)
      .mapValues(_.toList.sortBy(_._2).reverse.take(1))
      .map(_._2)
      .collect.foreach(println)
  }




  /**
    *
    * 求访问次数最多的资源文件
    *
    * 资源属于域名，求域名+资源的最多数
    *
    */
  def printMaxResource() = {
    //val lines = sc.textFile(fileName)
    //var arrayBuffer = ArrayBuffer()

    lines.map(x => {
      val record = x.split("\t")
      (Utils.getUrlIgnoreParam(record(LogAnalyzer.RESOURCE_IDX).trim), 1)
    }).reduceByKey(_+_)
      .sortBy(_._2, false).take(1).foreach(println)

  }

}


object LogAnalyzer {

  val APP_NAME = "LogAnalyzer"

  val DOMAIN_IDX = 0 //域名
  val TRAFFIC_IDX = 1 //流量
  val RESOURCE_IDX = 2 //资源
  val IP_IDX = 3 //ip
  val GET_TIME = 4 //访问时间


  def apply(fileName: String): LogAnalyzer = new LogAnalyzer(fileName)


}
