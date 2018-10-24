package main.scala.com.spark.core.log

/**
  * @author PGOne
  * @ date 2018/10/24
  */
object SparkLogApp {


  def main(args: Array[String]): Unit = {

    val fileName = "file:///Users/bullfly/IdeaProjects/demo-test/data/access-201707.log"

    val log = LogAnalyzer(fileName)

    //列出每个域名的流量
    log.printTrafficByDomain()

    //列出省份的访问次数(降序)
    log.printPVByProvince()

    //求每个域名下访问数最多的文件资源
    log.printMaxPVResourceByDomain()


    //求访问次数最多的资源文件
    log.printMaxResource()

    //关闭SparkContext
    log.stopSparkContext

  }

}

