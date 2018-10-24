/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package main.scala.com.pgone.Streaming03

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ruozedata on 2018/9/12.
  */
object StreamingKafkaApp02 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingKafkaApp02")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val kafkaParams =  Map[String, String](
      "metadata.broker.list"->"192.168.199.151:9092"
    )
    val topics = "ruoze_kafka_streaming".split(",").toSet
    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    lines.map(_._2).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
