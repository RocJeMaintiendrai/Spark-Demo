package main.scala.com.zookeeper

/**
  * @author PGOne
  * @ date 2018/10/24
  */
case class OffsetRange(
                        topic: String,
                        partition: Int,
                        fromOffset: Long,
                        utilOffset: Long
                      );
