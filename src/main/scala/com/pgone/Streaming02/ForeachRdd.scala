package main.scala.com.pgone.Streaming02

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author PGOne
  * @ date 2018/10/20
  */
object ForeachRdd {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRdd")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val lines = ssc.socketTextStream("hadoop000",9988)
    val result = lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

    result.foreachRDD { rdd =>
      val connection = createConnection()
      rdd.foreach { record =>

        val word = record._1
        val count = record._2
        val sql = s"insert into wc(word,c) values($word,$count)"
        connection.createStatement().execute(sql)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

  def createConnection() ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadooop000:3306/g3","root","root")
  }

}
