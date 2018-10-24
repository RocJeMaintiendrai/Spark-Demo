/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package main.scala.com.pgone.Streaming04

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * 1) StreamingContext
  * 2) 从kafka中获取数据     （获取offset）
  * 3）根据业务进行逻辑处理
  * 4）将处理结果存到外部存储中  （保存offset）
  * 5）启动程序，等待程序结束
  *
  */
object JdbcOffsetApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("JdbcOffsetApp")
    val kafkaParams =  Map[String, String](
      "metadata.broker.list"-> ValueUtils.getStringValue("metadata.broker.list"),
      "auto.offset.reset" -> ValueUtils.getStringValue("auto.offset.reset"),
      "group.id" -> ValueUtils.getStringValue("group.id")
    )
    val topics = ValueUtils.getStringValue("kafka.topics").split(",").toSet
    val ssc = new StreamingContext(sparkConf,Seconds(10))   // new context

    DBs.setup()
    val fromOffsets = DB.readOnly{ implicit  session => {
      sql"select * from offsets_storage".map(rs => {
        (TopicAndPartition(rs.string("topic"),rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }
    }.toMap


    /**
      * 每次重启应用，由于采用了smallest，都是从头开始，这就有问题了
      *
      * true 变活
      */
    val messages =  if(fromOffsets.size == 0) {
      println("~~~~~~从头开始消费~~~~~~~~~")

      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    } else { // 从已保存的offset开始消费
      println("~~~~~~从以保存的offset开始消费~~~~~~~~~")

//    val fromOffsets = Map[TopicAndPartition, Long]()
    val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
  }

    messages.foreachRDD(rdd=>{
      println("若泽数据统计记录为："+ rdd.count())

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      offsetRanges.foreach(x =>{
        // TODO... 需要保存到MySQL中的值
        println(s"---${x.topic},${x.partition},${x.fromOffset},${x.untilOffset}--")

        DB.autoCommit{
          implicit session => {
            sql"replace into offsets_storage(topic,groupid,partitions,offset) values(?,?,?,?)"
              .bind(x.topic,ValueUtils.getStringValue("group.id"),x.partition,x.untilOffset).update().apply()
          }
        }
      })

      //      if(!rdd.isEmpty()) {
//        println("若泽数据统计记录为："+ rdd.count())
//      }


    })
    ssc.start()
    ssc.awaitTermination()
  }


}
