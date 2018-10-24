package main.scala.com.pgone.Streaming03
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author PGOne
  * @ date 2018/10/22
  */
object StreamingFlumeApp02 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingFlumeApp02")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = FlumeUtils.createPollingStream(ssc, "hadoop000",41414)

    // SparkFlumeEvent ==> String
    lines.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}
