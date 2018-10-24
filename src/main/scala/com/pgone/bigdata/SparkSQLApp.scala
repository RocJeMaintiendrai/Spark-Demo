//package com.pgone.bigdata
//
//import org.apache.avro.generic.GenericData.StringType
//import org.apache.spark.sql._
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//
///**
//  * @author PGOne
//  * @date 2018/10/07
//  */
//object SparkSQLApp {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//                  .master("local[2]")
//                    .appName("SparkSQLApp").getOrCreate()
//
//    val info = spark.sparkContext.textFile("")
//
//    info.map(_.split(",")).map(x=>Row(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toInt))
//
//    val structType = StructType(
//      Array(
//        StructField("id",IntegerType,true),
//        StructField("weight",IntegerType,true),
//        StructField("age",IntegerType,true)
//      )
//    )
//
////    val schemaString = "id name age"
////    val fields = schemaString.split(" ")=
////        .map(fieldName => StructField(fieldName,StringType,nullable = true))
////
////    val schema = StructType(fields)
////
////    val info = spark.createDataFrame(info,schema)
//
//
//
////    info.collect().foreach(println)
////
////    import spark.implicits._
////    val infoDF = info.map(_.split(" ")).map(x=>Info(x(0).toInt,x(2),x(2).toInt)).toDF()
////    infoDF.show()
//
//
//    spark.stop()
//
//  }
//
//  case class Info(id:Int,name: String,age:Int)
//
//
//
//}
