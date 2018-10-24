//package com.pgone.bigdata
//
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * @author PGOne
//  * @ date 2018/10/04
//  */
//object ReduceAndGroupByKey {
//
//  def main(args: Array[String]): Unit = {
//
//      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ReduceAndGroupByKey")
//      val sc = new SparkContext(sparkConf)
//
//      val rdd = sc.textFile("").flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_)
//      sc.textFile("").flatMap(_.split("\t")).map((_,1)).groupByKey().map(x=>(x._1,x._2.sum))
//      sc.stop()
//  }
//
//}
