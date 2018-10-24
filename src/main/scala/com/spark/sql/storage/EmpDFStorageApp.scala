package main.scala.com.spark.sql.storage

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author PGOne
  * @date 2018/10/24
  *      * 0901 需求: 使用DF实现对emp.txt数据按入职年份进行分区存储
  *
  *     数据源:
  *     data/emp.txt
  *     存储目标和格式：
  *       data/ruoze/emp/y=1980:
  *                             1980.txt
  *       data/ruoze/emp/y=1981:
  *                             1981.txt
  *
  *
  *   设计:
  *    分别使用read和textFile读取原始文件
  *    先生成 /ruoze/temp/emp/y=1980/part-00000-7c982a06-1802-41db-9a17-7ea8da660185-c000.csv
  *    再生成 /ruoze/emp/y=1980/1980.txt 并删除临时目录
  *
  *
  *   DataFrame:
  *     方法1：使用DF实现emp数据按入职年份分区存储(数据源本地，输出HDFS)
  *             (使用合并临时目录多文件文件到新目录生成xxx.txt格式)
  *     方法2：使用DF实现emp数据按入职年份分区存储(数据源本地，输出HDFS)
  *             (使用合并临时目录多文件文件到新目录生成xxx.txt格式)
  *

  */
object EmpDFStorageApp {



  def main(args: Array[String]): Unit = {

    //删除 测试方法1和2 生成的HDFS目录和文件
    HDFSFileOperation.deleteFile("/ruoze1")
    HDFSFileOperation.deleteFile("/ruoze2")


    //使用DF实现emp按入职年份分区存储
    empDFHDFSStorageV2()

    //使用DF实现emp按入职年份分区存储
    empDFHDFSStorageV1()


  }


  /**
    * 使用DF实现emp数据按入职年份分区存储(数据源本地，输出HDFS)
    *
    *
    */
  def empDFHDFSStorageV2() = {

    val spark = SparkSession.builder().appName("EmpDFStorageApp")
      .master("local[2]").getOrCreate()

    //输入目录
    val prefix = "/Users/bullfly/IdeaProjects/demo-test"
    val outputTempDir = "/ruoze2/temp/emp"
    val mergedDir = "/ruoze2/emp"

    val empDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(EmpConverter.structType)
      .load(s"file://${prefix}/data/emp.txt")

    empDF.select(empDF.col("*"), empDF.col(EmpConverter.HIREDATE_NAME).substr(0, 4).as("y"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("y")
      .format("csv")
      .option("delimiter", "\t")
      .save(outputTempDir)

    //合并临时目录下所有文件到新目录
    EmpConverter.mergeFile(outputTempDir, mergedDir)

    spark.stop

  }



  /**
    *
    * 使用DF实现emp数据按入职年份分区存储(数据源本地，输出HDFS)
    *
    *
    */
  def empDFHDFSStorageV1() = {

    val spark = SparkSession.builder().appName("EmpDFStorageApp")
      .master("local[2]").getOrCreate()

    //输入目录
    val prefix = "/Users/bullfly/IdeaProjects/demo-test"
    val outputTempDir = "/ruoze1/temp/emp"
    val mergedDir = "/ruoze1/emp"

    val empRDD = spark.sparkContext.textFile(s"file://${prefix}/data/emp.txt")

    val empDF = spark.createDataFrame(empRDD.map(line => {
      EmpConverter.convert(line)
    }).filter(row => !row.equals(Row(0))),
      EmpConverter.structType)

    //按入职年份存储到HDFS
    //.option("compression", "none")
    empDF.select(empDF.col("*"), empDF.col(EmpConverter.HIREDATE_NAME).substr(0, 4).as("y"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("y")
      .format("csv")
      .option("delimiter", "\t")
      .save(outputTempDir)

    //合并临时目录下所有文件到新目录
    EmpConverter.mergeFile(outputTempDir, mergedDir)

    spark.stop

  }




}

