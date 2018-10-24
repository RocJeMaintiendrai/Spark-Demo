/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.pgone.SparkSQL04

import org.apache.spark.sql.SparkSession

/**
  * @author PGOne
  * @date 2018/10/16
  */
object PvUvApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("PvUvApp")
      .getOrCreate()

    val log = Array(
      "2018-09-04,G301",
      "2018-09-04,G302",
      "2018-09-04,G303",
      "2018-09-04,G301",
      "2018-09-04,G301",
      "2018-09-04,G301",
      "2018-09-04,G301",
      "2018-09-04,G302",
      "2018-09-04,G303",
      "2018-09-04,G301",
      "2018-09-04,G301",
      "2018-09-04,G311"
    )
    import spark.implicits._
    //求每个用户观看的视频次数
    val rdd = spark.sparkContext.parallelize(log)
    val df = rdd.map(_.split(",")).map(x=>Log(x(0),x(1))).toDF()
    df.show(false)
    import org.apache.spark.sql.functions._
    df.groupBy("date","user").agg(count("user").as("pv"))
        .sort('pv.desc)
        .select("date","user","pv").show()

    val likes = spark.sparkContext.textFile("")

    val likeDF = likes.map(_.split("\t")).map(x=>Likes(x(0),x(1))).toDF()
    likeDF.createOrReplaceTempView("t_likes")

    /**
      * 1)定义函数
      * 2）使用函数
      */
    spark.udf.register("likes_num",(x:String) => x.split(",").size)
    spark.sql("select name,likes,likes_num() from t_likes")
    spark.stop()
  }

  case class Log(date:String, user:String)
  case class Likes(name:String, likes:String)

}