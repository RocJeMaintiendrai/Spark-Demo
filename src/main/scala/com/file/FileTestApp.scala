package main.scala.com.file

import java.io.{File, PrintWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import scala.util.Random

/**
  * @author PGOne
  * @ date 2018/10/24
  */
object FileTestApp {

  /**
    *
    * @return Get domain randomly(3 domains)
    */
  def mockDomain(): String = {

    val domainArray = Array(
      "www.ruozedata.com",
      "www.zhibo8.com",
      "www.dongqiudi.com",
      "www.youku.com",
      "www.bilibili.com")

    domainArray(Random.nextInt(domainArray.length))

  }

  /**
    *
    * @return Return 500-50000 random flow,sometimes non-numeric String
    */
  def mockTraffic(): String = {
    val stepValue = Random.nextInt(501) * 10
    val expStringArray = Array("AAA", "1BB", "CC1", "#DD", "EE$", "#$%")

    if (stepValue <= 25 * 10)
      expStringArray(Random.nextInt(expStringArray.length))
    else
      (500 + stepValue).toString

  }

  /**
    *
    * random time format: [2017/01/01 00:00:00]
    *
    * @param startTime starting time stamp
    * @return random timestamp in 30 days
    */
  def mockTime(startTime: Long): String = {

    val dateFormat = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]")
    dateFormat.format(new Date(startTime + Random.nextInt(30 * 24 * 60) * 1000 * 60L))

  }


  /**
    *
    * @return get several IP randomly
    */
  def mockIp(): String = {

    val ipArray = Array(
      "192.168.1.1",
      "172.0.0.1",
      "172.10.0.1",
      "172.20.0.1",
      "172.30.0.1",
      "10.0.0.1",
      "10.10.0.1",
      "10.20.0.1",
      "10.30.0.1")

    ipArray(Random.nextInt(ipArray.length))

  }


  /**
    *
    * @return get several resources randomly
    */
  def mockResource(): String = {

    val resourceArray = Array(
      "pdf/tech/scala.pdf",
      "video/tech/scala.mp4",
      "pdf/music/gitar.pdf",
      "pdf/music/djembe.pdf",
      "pdf/music/bass.pdf?key1=value1&key2=value2",
      "video/music/bass.mp4?key1=value1",
      "video/music/gitar.mp4",
      "video/music/djembe.mp4",
      "pdf/tech/python.pdf",
      "video/tech/python.mp4",
      "pdf/tech/go.pdf",
      "video/tech/go.mp4")

    resourceArray(Random.nextInt(resourceArray.length))

  }

  /**
    * domain resource flow ip time
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val startTime = 1498838400000L // 2017-07-01 00:00:00
    val fileName = "data/access-201707.log"
    var domain = ""

    val writer = new PrintWriter(new File(fileName))

    for (e <- 1 to 10000) {
      domain = mockDomain
      writer.write(s"${domain}\t${mockTraffic}\thttps://${domain}/${mockResource}\t${mockIp}\t${mockTime(startTime)}")
      writer.write("\n")
    }

    writer.close()
  }
}

