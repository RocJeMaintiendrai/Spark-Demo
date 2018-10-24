package main.scala.com.spark.core.rdd

import better.files.{File => ScalaFile, _}

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map


/**
  * @author PGOne
  * @ date 2018/10/24
  */
object EmpRDDStorageApp {

  val EMP_NO = 0 //员工编号
  val ENAME = 1 //员工名字
  val JOB = 2 //员工工作
  val MGR = 3 //上级编号
  val HIREDATE = 4 //入职日期
  val SAL = 5 //薪水
  val COMM = 6 //奖金
  val DEPTNO =7 //部门编号


  def main(args: Array[String]): Unit = {

    //////////////////删除测试方法生成的目录和文件//////////////////////

    //删除测试方法1生成的本地目录和文件
    val dataDir1 = ScalaFile("data/ruoze1")
    if(dataDir1.exists) {
      dataDir1.delete()
    }
    //删除测试方法2生成的本地目录和文件
    val dataDir2 = ScalaFile("data/ruoze2")
    if(dataDir2.exists) {
      dataDir2.delete()
    }

    //删除测试方法3生成的-HDFS输出目录和文件
    HDFSFileOperation.deleteFile("/ruoze3")
    //删除测试方法4生成的-HDFS输出目录和文件
    HDFSFileOperation.deleteFile("/ruoze4")

    //测试2方法: 使用SparkContext textFile读取文件，按指定子目录存储到本地文件夹
    empRDDLocalStorage()

    //测试3方法: 使用SparkContext textFile读取文件，按指定子目录存储到HDFS
    empRDDHDFSStorage()

    //测试1方法: 使用文件接口读取文件，按指定子目录存储到本地文件夹
    empFileLocalStorage()


  }




  /**
    *
    * 测试2：使用RDD实现emp数据按入职年份分区存储(数据源本地，输出本地)
    *
    */
  def empRDDLocalStorage() = {

    val sparkConf = new SparkConf().setAppName("EmpRDDStorageApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val dataDir = "/Users/bullfly/IdeaProjects/demo-test/data"
    //val dataDir = "/home/hadoop/data/test"
    val outputDir = "data/ruoze2/emp"

    val lines = sc.textFile(s"file://${dataDir}/emp.txt")

    lines.map(x => {
      val cs = x.split("\t")
      (cs(HIREDATE).split("-")(0), x) //(入职年份, 员工信息)
    }).partitionBy(new YearSparkPartition(5)) //5个分区
      .foreachPartition( e => {

      val fileMap = Map[String, ScalaFile]()

      for((key, value) <- e) {
        //目录不存在则创建
        ScalaFile(s"${outputDir}/y=${key}").createDirectoryIfNotExists()
        //从map中获取对象->不存在则创建并丢入map->检查是否有文件没有则创建->然后放入一行数据
        fileMap.getOrElseUpdate(key, ScalaFile(s"${outputDir}/y=${key}/${key}.txt"))
          .createFileIfNotExists().appendLine(value)
      }

    })

    sc.stop


  }


  /**
    *
    * 测试3：使用RDD实现emp数据按入职年份分区存储(数据源本地，输出HDFS)
    *
    */
  def empRDDHDFSStorage() = {
    val sparkConf = new SparkConf().setAppName("EmpRDDStorageApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val dataDir = "/Users/bullfly/IdeaProjects/demo-test/data"
    //val dataDir = "/home/hadoop/data/test"
    val lines = sc.textFile(s"file://${dataDir}/emp.txt")

    lines.map(x => {
      val cs = x.split("\t")
      //(入职年份, 员工信息)
      (cs(HIREDATE).split("-")(0), x)
    }).partitionBy(new YearSparkPartition(5))
      .saveAsHadoopFile("/ruoze3/emp",
        classOf[String],
        classOf[String],
        classOf[YearMultipleTextOutputFormat])

    sc.stop

  }


  /**
    *
    * 测试1：使用文件接口实现emp数据按入职年份分区存储(数据源本地，输出本地)
    *
    */
  def empFileLocalStorage(): Unit = {
    val logger = Logger.getLogger(EmpRDDStorageApp.getClass.getName)

    val dataDir = "data"
    val inputFile = "emp.txt"
    var outputSubDir = "ruoze1"

    val file = (dataDir/inputFile)

    if(file.exists) {
      file.lines.map(x => {
        val cs = x.split("\t")
        val year = cs(HIREDATE).split("-")(0)
        val dirPath = (dataDir/outputSubDir/"emp"/s"y=${year}")

        //输出目录如果不存在则创建多级目录
        dirPath.createDirectoryIfNotExists()

        //输出目录的文件,存在则先删除再创建
        (dirPath/s"${year}.txt")
          .createFileIfNotExists()
          .appendLine(
            s"${cs(EMP_NO)}\t${cs(ENAME)}\t${cs(JOB)}\t${cs(MGR)}\t${cs(HIREDATE)}\t${cs(SAL)}\t${cs(COMM)}\t${cs(DEPTNO)}")
      })
    } else {
      logger.warn(s"file does not exist: file:${file.path}")
    }

  }
}
