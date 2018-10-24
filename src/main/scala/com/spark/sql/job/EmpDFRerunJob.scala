package main.scala.com.spark.sql.job
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
/**
  * @author PGOne
  * @ date 2018/10/24
  *
  * 0902 需求: 使用DF实现对emp.txt数据按入职年份进行分区存储(支持幂等性)
  *
  *             1.原始数据目录:多份文件存储不同的雇员信息
  *               hdfs://ct01.bullfly.cc/data/resource/emp/emp-1.txt
  *                                                        emp-2.txt
  *
  *             2.按入职年份进行分区存储
  *                hdfs://ct01.bullfly.cc/ruoze/emp/y=1987/1987-1.txt
  *                                                        1987-2.txt
  *                                                 y=1988/1988-1.txt
  *                                                        1988-2.txt
  *
  *             3.支持多次重跑不重复
  *
  *
  *       实现方法:
  *         1.根据原始文件的数据按分区存储到临时文件夹
  *           hdfs://ct01.bullfly.cc/ruoze/temp/emp-1.txt/emp/y=1987:
  *                   part-00000-7c982a06-1802-41db-9a17-7ea8da660185-c000.csv
  *                   part-00001-7c982a06-1802-41db-9a17-7ea8da660185-c000.csv
  *           hdfs://ct01.bullfly.cc/ruoze/temp/emp-2.txt/emp/y=1987:
  *                   part-00000-7c982a06-1802-41db-9a17-7ea8da660185-c000.csv
  *                   part-00001-7c982a06-1802-41db-9a17-7ea8da660185-c000.csv
  *         2.检查历史是否已经处理过该原始文件
  *         3.处理过则删除对应的文件
  *         4.合并文件到新目录
  *           hdfs://ct01.bullfly.cc/ruoze/emp/y=1987/1987-1.txt
  *         5.删除临时目录

  */
object EmpDFRerunJob {


  def main(args: Array[String]): Unit = {

    empStorageJobRerun("emp-1.txt")
    empStorageJobRerun("emp-2.txt")

  }



  /**
    *
    * 使用DF实现emp数据按入职年份分区存储(数据源本地，输出HDFS)
    *
    *  @param file 重跑的文件名 文件名格式约定 xxx-1.txt
    *
    *
    */
  def empStorageJobRerun(file: String) = {

    val pattern = "[\\w\\d]+-\\d+\\.txt".r
    if(!file.matches(pattern.toString())) throw new ConvertException("invalid filename")

    val fileDiv = file.split("-")
    val obj = fileDiv(0)
    val idx = fileDiv(1).split("\\.")(0)

    val spark = SparkSession.builder().appName("EmpDFStorageApp")
      .master("local[2]").getOrCreate()

    //输入目录
    val inputDir = s"/data/resource/${obj}"
    val outputTempDir = s"/ruoze/temp/${file}/${obj}"
    val mergedDir = s"/ruoze/${obj}"

    val empDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(EmpConverter.structType)
      .load(s"${inputDir}/${file}")

    //1.根据原始文件的数据按分区存储到临时文件夹
    empDF.select(empDF.col("*"), empDF.col(EmpConverter.HIREDATE_NAME).substr(0, 4).as("y"))
      //.foreach(println(_))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("y")
      .format("csv")
      .option("delimiter", "\t")
      .save(outputTempDir)

    //2.检查历史是否已经处理过该原始文件，处理过则删除对应的所有文件
    EmpConverter.deleteTargetFileByTempWithIdx(outputTempDir, mergedDir, idx)

    //3.合并临时目录的文件到新目录并删除临时目录
    EmpConverter.mergeFileWithIdx(outputTempDir, mergedDir, idx)

    spark.stop

  }

}
