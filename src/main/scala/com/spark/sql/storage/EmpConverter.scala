package main.scala.com.spark.sql.storage


import main.scala.com.Util.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import main.scala.com.spark.sql.storage.HDFSFileOperation.{deleteFile, listMatchedDirectoryNames, merge}


/**
  * @author PGOne
  * @ date 2018/10/24
  */
object EmpConverter {

  val logger = Logger.getLogger(EmpConverter.getClass.getName)

  private val LENGTH = 8 //总字段数
  private val EMP_NO = 0 //员工编号
  private val ENAME = 1 //员工名字
  private val JOB = 2 //员工工作
  private val MGR = 3 //上级编号
  private val HIREDATE = 4 //入职日期
  private val SAL = 5 //薪水
  private val COMM = 6 //奖金
  private val DEPTNO = 7 //部门编号

  val HIREDATE_NAME = "hiredate"

  //输出字段定义
  val structType = StructType(Array(
    StructField("emp_no", IntegerType, false),
    StructField("ename", StringType, true),
    StructField("job", StringType, true),
    StructField("mgr", IntegerType, true),
    StructField("hiredate", StringType, true),
    StructField("sal", DoubleType, true),
    StructField("comm", DoubleType, true),
    StructField("deptno", IntegerType, true)))


  /**
    * 输入转输出
    *
    * @param line 输入 字符串以\t分割
    * @return Row
    */
  def convert(line: String) = {

    try {
      val cs = line.split("\t")

      if (LENGTH != cs.length) throw new ConvertException("failed to convert line")

      Row(cs(EMP_NO).toInt, cs(ENAME), cs(JOB), Utils.getInt(cs(MGR)), cs(HIREDATE),
        Utils.getDouble(cs(SAL)), Utils.getDouble(cs(COMM)), cs(DEPTNO).toInt)

    } catch {
      case e: Exception => {
        logger.warn(e.getMessage)
        Row(0)
      }
    }
  }


  /**
    *
    * 根据日期字符串返回年份 1910-12-17
    *
    * @param str
    * @return
    */
  def getDate(str: String)= {
    val pattern = "\\d{4}-\\d{2}-\\d{2}".r
    if(!str.matches(pattern.toString())) throw new ConvertException("failed to convert date")

    val date = str.split("-")
    (date(0), date(1), date(2))
  }


  /**
    * 将指定目录下的y=1980/part-00000 part-00001文件合并成指定目录的y=1980/1980.txt
    *
    */
  def mergeFile(outputTempDir: String, mergedDir: String) = {

    logger.debug(s"mergeFile ${outputTempDir} to ${mergedDir}")

    val tempList = listMatchedDirectoryNames(s"${outputTempDir}/y=*")
    tempList.foreach( temp => {
      val targetFile = temp.split("=")(1)
      merge(s"${outputTempDir}/${temp}", s"${mergedDir}/${temp}/${targetFile}.txt")
    })

    deleteFile(s"${outputTempDir}")

  }

  def main(args: Array[String]): Unit = {

    val line = convert("7698\tBLAKE\tMANAGER\t7839\t1915-5-1\t2850.00\t\t30")
    println(line)

    println(getDate("1910-12-17"))

  }

}

