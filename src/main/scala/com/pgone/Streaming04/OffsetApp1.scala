package main.scala.com.pgone.Streaming04

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author PGOne
  * @ date 2018/10/22
  */
object OffsetApp1 {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("OffsetApp1")
    val kafkaParams =  Map[String, String](
      "metadata.broker.list"->"192.168.199.151:9092",
      "auto.offset.reset" -> "smallest"
    )
    val topics = "ruoze_g3_offset".split(",").toSet
    val checkpointDirectory = "hdfs://hadoop000:8020/g3_offset/"

    // Function to create and setup a new StreamingContext
    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(sparkConf,Seconds(10))   // new context
      val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
      ssc.checkpoint(checkpointDirectory)
      messages.checkpoint(Duration(8*10*1000))

      messages.foreachRDD(rdd=>{
        if(!rdd.isEmpty()) {
          println("若泽数据统计记录为："+ rdd.count())
        }
      })

      ssc
    }


    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)



    ssc.start()
    ssc.awaitTermination()

}

}
