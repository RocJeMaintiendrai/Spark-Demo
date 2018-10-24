package main.scala.com.spark.sql.logstats

import org.apache.log4j.Logger
import org.apache.spark.sql.types._


/**
  * @author PGOne
  * @ date 2018/10/24
  *      日志信息转换
  *
  * domain  traffic url ip  access_time
  * www.dongqiudi.com	5140	https://www.dongqiudi.com/pdf/music/bass.pdf?key1=value1&key2=value2	172.30.0.1	[2017-07-29 12:34:00]
  *

  */
object LogConverter {

  val logger = Logger.getLogger(LogConverter.getClass.getName)

  val C_LENGTH = 5 //总字段数
  val IDX_DOMAIN = 0 //域名
  val IDX_TRAFFIC = 1 //流量(存在非法字符)
  val IDX_URL = 2 //访问资源
  val IDX_IP = 3 //IP
  val IDX_ACCESS_TIME = 4 //访问时间( [2017/01/01 00:00:00] )

  val C_DOMAIN = "domain"
  val C_TRAFFIC = "traffic"
  val C_URL = "url"
  val C_IP = "ip"
  val C_ACCESS_TIME = "access_time"


  val structType = StructType(Array(
    StructField(C_DOMAIN, StringType, false),
    StructField(C_TRAFFIC, StringType, true),
    StructField(C_URL, StringType, true),
    StructField(C_IP, StringType, true),
    StructField(C_ACCESS_TIME, StringType, true)))


  /**
    *
    * 日志文件访问时间的特殊转换
    * [2017/01/01 00:00:00] to 2017-01-01 00:00:00
    * @param str
    *
    */
  private def getDate(str: String) = {
    str.replaceAll("/", "-").replaceAll("[\\[\\]]{1}", "")
  }

}

