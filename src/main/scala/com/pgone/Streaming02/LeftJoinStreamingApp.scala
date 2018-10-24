/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package main.scala.com.pgone.Streaming02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @author PGOne
  * @ date 2018/10/20
  */
object LeftJoinStreamingApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LeftJoinStreamingApp")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    //from rdd
    val input2 = new ListBuffer[(String,Boolean)]
    input2.append(("www.baidu.com",true))
    val data2 = ssc.sparkContext.parallelize(input2)

    //from nc
    val lines = ssc.socketTextStream("hadoop000",8888)
    lines.map(x=>(x.split(",")(0),x)).transform(rdd => {
      rdd.leftOuterJoin(data2).filter(x=>{
        x._2._2.getOrElse(false) != true
      }).map(_._2._1)
    }).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
