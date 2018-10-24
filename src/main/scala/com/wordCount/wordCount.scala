/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package main.scala.com.wordCount

import scala.io.Source
/**
  * @author PGOne
  * @ date 2018/10/24
  */
object WordCountApp {

  def main(args: Array[String]): Unit = {

    val lines = List("python python c scala","scala php jsp vue react vue", "python", "scala go scala", "scala scala ruby nodejs nodejs")
    val wc1 = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).toList.sortBy(_._2).reverse
    val wc2 = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.size).toList.sortBy(_._2).reverse
    val wc3 = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList.sortBy(_._2).reverse

    println(wc1)
    println(wc2)
    println(wc3)

    val file  = Source.fromFile("data.txt")
    val lines2 = file.getLines().toList
    val wc4 = lines2.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).toList.sortBy(_._2).reverse
    println(wc4)


    //Spark方式
    //val wc5 = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect

  }
}
