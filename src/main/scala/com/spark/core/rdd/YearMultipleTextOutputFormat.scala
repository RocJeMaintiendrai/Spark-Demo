package main.scala.com.spark.core.rdd

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat


/**
  * @author PGOne
  * @ date 2018/10/24
  */
class YearMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  /**
    *
    * 自定义输出文件格式为
    *
    * y=${key}/${key}.txt 用于在指定目录生成y=1980/1980.txt文件
    *
    * @param key
    * @param value
    * @param name
    * @return
    */
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val year = key.asInstanceOf[String]
    //println("xxxxxxxxxxxxxxxx"+key+":"+name)
    //Thread.sleep(10)
    //s"y=${year}/${name}"
    s"y=${year}/${year}.txt"
  }

  /**
    *
    * 设置返回的内容不含key(year)
    *
    * @param key
    * @param value
    * @return
    */
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }
}
