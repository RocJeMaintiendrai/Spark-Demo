package main.scala.com.zookeeper

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.io.retry.RetryPolicy
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat

import scala.collection.mutable.ArrayBuffer


/**
  * @author PGOne
  * @ date 2018/10/24
  */
object OffsetRangeTestApp extends App {

  val groupName = "G327"
  val topicName = "ruoze_offset_topic"
  val partitionNum = 10

  val offsetRangeManager = OffsetRangeManager()

  //构建3个分区数据并存储到ZK
  var parArrBuffer = ArrayBuffer[OffsetRange]()

  for(e <- 0 until partitionNum) {
    parArrBuffer += OffsetRange(topicName, e, 0, 100)
  }
  offsetRangeManager.store(parArrBuffer.toArray, groupName)

  //获取刚才创建的分区数据
  val offsetRangeMap = offsetRangeManager.obtain(topicName, groupName)
  offsetRangeMap.foreach( m => {
    println(s"create node -> topic:${m._1.topic} partition:${m._1.partition} data: ${m._2}")
  })


  //更新3个分区的新版本数据到ZK
  var parArrBufferForUpdate = ArrayBuffer[OffsetRange]()

  for(e <- 0 until partitionNum) {
    parArrBufferForUpdate += OffsetRange(topicName, e, 0, 999)
  }
  offsetRangeManager.update(parArrBufferForUpdate.toArray, groupName)


  //获取刚才更新的分区数据
  val offsetRangeMapUpdated = offsetRangeManager.obtain(topicName, groupName)
  offsetRangeMapUpdated.foreach( m => {
    println(s"update node -> topic:${m._1.topic} partition:${m._1.partition} data: ${m._2}")
  })


}

