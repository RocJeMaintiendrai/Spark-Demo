package main.scala.com.Util

/**
  * @author PGOne
  * @ date 2018/10/24
  */
object ComUtil {

  /**
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

  def getProvince(ip: String): String = {
    val ipArray = ip.split('.')
    ipMap.getOrElse(s"${ipArray(0)}.${ipArray(1)}", "湖南")
  }


  /**
    *
    * 域名/资源?参数 -> 返回资源
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
    * 域名/资源?参数 -> 返回域名/资源
    * @param url
    * @return
    */
  def getUrlIgnoreParam(url: String) = {
    val resource = url.trim.replaceFirst("https://", "")
    val endFlag = resource.indexOf("?")
    resource.substring(0, if(endFlag == -1) resource.length else endFlag)
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
