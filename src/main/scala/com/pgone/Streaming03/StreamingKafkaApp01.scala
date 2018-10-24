/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package main.scala.com.pgone.Streaming03

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ruozedata on 2018/9/12.
  */
object StreamingKafkaApp01 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingKafkaApp01")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val topics = "ruoze_kafka_streaming".split(",").map((_,1)).toMap
    val lines = KafkaUtils.createStream(ssc, "hadoop000:2181","ruoze_group",topics)
    lines.map(_._2).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
