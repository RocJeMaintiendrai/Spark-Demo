package main.scala.com.pgone.Streming01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author PGOne
  * @ Date 2018/10/19
  */
object StremingWCApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingWCApp")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    //已有ssc，要进行实时流处理，需要源头的DStream
    val lines = ssc.socketTextStream("hadoop000",8888)
    //transformation
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)

    //action
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
