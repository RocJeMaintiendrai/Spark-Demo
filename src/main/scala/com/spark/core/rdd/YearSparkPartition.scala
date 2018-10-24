package main.scala.com.spark.core.rdd


import main.scala.com.Util.Utils
import org.apache.spark.Partitioner


/**
  * @author PGOne
  * @ date 2018/10/24
  * partition according to year
  */
class YearSparkPartition(numParts: Int) extends Partitioner {

  //创建分区的个数
  override def numPartitions: Int = numParts


  /**
    * 对输入的key做计算，然后返回该key的分区ID，0到numPartitions-1
    *
    * 根据hashCode和numPartitions取余来得到Partition
    *
    * @param key
    * @return
    */
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.asInstanceOf[String].hashCode, numPartitions)
  }

  /**
    *
    * 判断对象相等的函数，用于Spark内部比较两个RDD的分区是否一样
    *
    * 实例是EmpYearSparkPartition的实例，并且numPartitions相同，返回true，否则返回false
    *
    * @param other
    * @return
    */
  override def equals(other: Any): Boolean = other match {
    case yearSparkPartition: YearSparkPartition =>
      yearSparkPartition.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

}
