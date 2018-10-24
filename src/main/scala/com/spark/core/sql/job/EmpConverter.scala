package main.scala.com.spark.core.sql.job


import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import main.scala.com.spark.core.sql.job.HDFSUtils.{deleteFile, listMatchedDirectoryNames, merge}

/**
  * @author PGOne
  * @ date 2018/10/24
  * 雇员信息转换工具类
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

  val structType = StructType(Array(
    StructField("emp_no", IntegerType, false),
    StructField("ename", StringType, true),
    StructField("job", StringType, true),
    StructField("mgr", IntegerType, true),
    StructField("hiredate", StringType, true),
    StructField("sal", DoubleType, true),
    StructField("comm", DoubleType, true),
    StructField("deptno", IntegerType, true)))


  //  /**
  //    * 输入转输出
  //    * 采用csv方式读写不需要这个转换，DataFrame&RDD编程方式才需要输入输出转换
  //    * @param line 输入 字符串以\t分割
  //    * @return Row
  //    */
  //  def convert(line: String) = {
  //
  //    try {
  //      val cs = line.split("\t")
  //
  //      if (LENGTH != cs.length) throw new ConvertException("failed to convert line")
  //
  //      Row(cs(EMP_NO).toInt, cs(ENAME), cs(JOB), Utils.getInt(cs(MGR)), cs(HIREDATE),
  //        Utils.getDouble(cs(SAL)), Utils.getDouble(cs(COMM)), cs(DEPTNO).toInt)
  //
  //    } catch {
  //      case e: Exception => {
  //        logger.warn(e.getMessage)
  //        Row(0)
  //      }
  //    }
  //  }


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

    logger.info(s"deleteFile ${outputTempDir}")
    deleteFile(s"${outputTempDir}", true)

  }


  /**
    * 将指定目录下的y=1980/part-00000 part-00001文件合并成指定目录的y=1980/1980-1.txt
    *
    */
  def mergeFileWithIdx(outputTempDir: String, mergedDir: String, idx: String) = {

    logger.info(s"mergeFileWithIdx ${outputTempDir} to ${mergedDir}")

    val tempList = listMatchedDirectoryNames(s"${outputTempDir}/y=*")
    tempList.foreach( temp => {
      val targetFile = temp.split("=")(1)
      merge(s"${outputTempDir}/${temp}", s"${mergedDir}/${temp}/${targetFile}-${idx}.txt")
    })

    logger.info(s"deleteFile ${outputTempDir}")
    deleteFile(s"${outputTempDir}", true)

  }


  /**
    *
    * 根据重跑的信息删除上一次生成的文件
    *
    * @param outputTempDir 输出临时目录
    * @param mergedDir 合并目录
    * @param idx 文件编号
    */
  def deleteTargetFileByTempWithIdx(outputTempDir: String, mergedDir: String, idx: String) = {

    logger.info(s"outputTempDir: ${outputTempDir}")

    val fileParentList = HDFSUtils.listMatchedDirectoryNames(s"${outputTempDir}/y=*")
    fileParentList.foreach(fileParent => {
      //删除对应的文件
      val year = fileParent.split('=')(1)
      logger.info(s"deleteTargetFileByTemp ${mergedDir}/${fileParent}/${year}-${idx}.txt")
      HDFSUtils.deleteFile(s"${mergedDir}/${fileParent}/${year}-${idx}.txt")
    })
  }

  def main(args: Array[String]): Unit = {

    //    val line = convert("7698\tBLAKE\tMANAGER\t7839\t1915-5-1\t2850.00\t\t30")
    //    println(line)
    //
    //    println(getDate("1910-12-17"))

  }

}
