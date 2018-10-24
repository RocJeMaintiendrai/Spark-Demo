package main.scala.com.Util

/**
  * @author PGOne
  * @ date 2018/10/24
  */
/**
  * Created by IntelliJ IDEA.
  *
  * User: bullfly
  * Date: 18/8/24
  * Description:
  *
  * 一些通用的字符串和数字的转换工具以及一些测试用的硬编码
  *
  */
object Utils {

  /**
    *
    * 测试用硬编码
    *
    * "192.168.1.1",
    * "172.0.0.1",
    * "172.10.0.1",
    * "172.20.0.1",
    * "172.30.0.1",
    * "10.0.0.1",
    * "10.10.0.1",
    * "10.20.0.1",
    * "10.30.0.1"
    */
  val ipMap = Map(
    "192.168"->"黑龙江",
    "172.0"->"吉林",
    "172.10"->"福建",
    "172.20"->"内蒙古",
    "172.30"->"新疆",
    "10.0"->"浙江",
    "10.10"->"辽宁",
    "10.20"->"宁夏",
    "10.20"->"广西",
    "172.30"->"广东"
  )

  /**
    * 测试用硬编码
    *
    * @param ip
    * @return
    */
  def getProvince(ip: String): String = {
    val ipArray = ip.split('.')
    ipMap.getOrElse(s"${ipArray(0)}.${ipArray(1)}", "湖南")
  }


  /**
    *
    * "域名/资源?参数" -> 返回"资源"
    *
    * @param url
    * @return
    */
  def getResource(url: String) = {
    var resource = url.trim.replaceFirst("//", "")
    val startFlag = resource.indexOf("/")
    val endFlag = resource.indexOf("?")

    if(startFlag == -1) {
      resource = ""
    } else {
      val endFlag = resource.indexOf("?")
      resource = resource.substring(startFlag, if(endFlag == -1) resource.length else endFlag)
    }
    resource

  }


  /**
    * "域名/资源?参数" -> 返回"域名/资源"
    * @param url
    * @return
    */
  def getUrlIgnoreParam(url: String) = {
    val resource = url.trim.replaceFirst("https://", "")
    val endFlag = resource.indexOf("?")
    resource.substring(0, if(endFlag == -1) resource.length else endFlag)
  }



  /**
    *
    * 返回的必须是非负数(对于hashCode如果为负的情况做了特殊处理)
    *
    * Calculates 'x' modulo 'mod', takes to consideration sign of x,
    * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
    * so function return (x % mod) + mod in that case.
    * @param x
    * @param mod
    * @return
    */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  /**
    *
    * 根据字符串返回double
    *
    * @param str
    * @param default
    * @return
    */
  def getDouble(str: String, default: Double = 0.0) = {
    try {
      if(str.trim.equals("")) default else str.trim.toDouble
    } catch {
      case _: Exception => 0.0
    }
  }



  /**
    *
    * 根据字符串返回int
    *
    * @param str
    * @param default
    * @return
    */
  def getInt(str: String, default: Int = 0) = {
    try {
      if(str.trim.equals("")) default else str.trim.toInt
    } catch {
      case _: Exception => 0
    }
  }

  /**
    *
    * 根据字符串返回Long
    *
    * @param str
    * @param default
    * @return
    */
  def getLong(str: String, default: Long = 0) = {
    try {
      if(str.trim.equals("")) default else str.trim.toLong
    } catch {
      case _: Exception => 0L
    }
  }


  def main(args: Array[String]): Unit = {
    //    println(getProvince("192.168.1.1"))
    //    println(getProvince("10.168.1.1"))
    //    println(getProvince("10.0.1.1"))
    //    println(getProvince("10.10.1.1"))
    //    println(getProvince("172.0.1.1"))

    println(getResource("https://test.com"))
    println(getResource("https://test.com/"))
    println(getResource("https://test.com/test/xxx.mp4"))
    println(getResource("https://test.com/test/xxx.mp4?sadfas=asdfsa&ewffds=sss"))

    //    println(getUrlIgnoreParam("https://test.com"))
    //    println(getUrlIgnoreParam("https://test.com/"))
    //    println(getUrlIgnoreParam("https://test.com/test/xxx.mp4?sadfas=asdfsa&ewffds=sss"))
  }



}
