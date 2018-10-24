package main.scala.com.pgone.Streaming02

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @author PGOne
  * @ date 2018/10/20
  */
object LeftJoinApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("LeftJoinApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf);

    val input1 = new ListBuffer[(String,Long)]
    input1.append(("www.ruozedata.com",8888))
    input1.append(("www.ruozedata.com",9999))
    input1.append(("www.ruozedata.com",7777))
    val data1 = sc.parallelize(input1)

    val input2 = new ListBuffer[(String,Boolean)]
    input2.append(("www.baidu.com",true))
    val data2 = sc.parallelize(input2)
    data1.leftOuterJoin(data2)
          .filter(x=>{x._2._2.getOrElse(false) != true})
          .map(x=>(x._1,x._2._1))
          .collect().foreach(println)


    sc.stop()

  }

}
