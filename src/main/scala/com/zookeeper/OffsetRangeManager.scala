package main.scala.com.zookeeper
import com.typesafe.config.ConfigFactory
/**
  * @author PGOne
  * @ date 2018/10/24
  */
class OffsetRangeManager {

  // 获取配置信息
  val config =  ConfigFactory.load()
  val offsetConfig = config.getConfig("zookeeper.offset")

  val rootName = offsetConfig.getString("rootName")
  val subName = offsetConfig.getString("subName")
  val partitionPName = offsetConfig.getString("partitionPName")

  val zkNodeManager = ZKNodeManager()


  /**
    * 构建zk存储path
    *
    * @param rootName
    * @param groupName
    * @param subName
    * @param topic
    * @param partitionPName
    * @param partition 默认值:-1 构建的path不含有具体第几分区
    * @return
    */
  def genPath(
               rootName: String,
               groupName: String,
               subName: String,
               topic: String,
               partitionPName: String,
               partition: Int = -1) = {
    if(partition < 0)
      s"/${rootName}/${groupName}/${subName}/${topic}/${partitionPName}"
    else
      s"/${rootName}/${groupName}/${subName}/${topic}/${partitionPName}/${partition}"
  }

  /**
    *
    * 保存offsetRange对象数组(新增节点并存储数据)
    *
    * @param offsetRanges
    * @param groupName
    */
  def store(
             offsetRanges:Array[OffsetRange],
             groupName:String) = {

    offsetRanges.foreach(e => {
      val path = genPath(rootName, groupName, subName, e.topic, partitionPName, e.partition)
      zkNodeManager.createNode(path, e.utilOffset.toString)
    })

  }

  /**
    *
    * 保存offsetRange对象数组(更新节点数据)
    *
    * @param offsetRanges
    * @param groupName
    */
  def update(
              offsetRanges:Array[OffsetRange],
              groupName:String) = {

    offsetRanges.foreach(e => {
      val path = genPath(rootName, groupName, subName, e.topic, partitionPName, e.partition)
      zkNodeManager.updateNode(path, e.utilOffset.toString)
    })

  }

  /**
    *
    * 获取指定topic所有分区数据
    *
    * @param topic
    * @param groupName
    * @return
    */
  def obtain(
              topic:String,
              groupName:String): Map[TopicAndPartition, Long] = {

    val path = s"/${rootName}/${groupName}/${subName}/${topic}/${partitionPName}"
    val partitions = zkNodeManager.listNodes(path)
    var map = Map[TopicAndPartition, Long]()
    var subPath = ""

    partitions.foreach(p => {
      subPath = s"${path}/${p}"
      map += (TopicAndPartition(topic, p.toInt) -> zkNodeManager.getNodeDataAndStat(subPath)._1.toLong)
    })

    map
  }

}

object OffsetRangeManager {

  def apply(): OffsetRangeManager = new OffsetRangeManager()

}
