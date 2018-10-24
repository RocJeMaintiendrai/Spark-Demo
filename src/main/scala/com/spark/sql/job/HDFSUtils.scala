package main.scala.com.spark.sql.job

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocatedFileStatus, Path}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * @author PGOne
  * @ date 2018/10/24
  */
object HDFSUtils {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)


  /**
    * 正则方式列出符合的目录名称，不含父目录
    *
    * /ruoze/temp/emp-1\.txt/emp/y=*
    * ==>
    * y=1910
    *
    * @param regexPath
    * @return
    */
  def listMatchedDirectoryNames(regexPath: String) = {
    val dirNameList = ListBuffer[String]()
    val fileStatus = fileSystem.globStatus(new Path(regexPath))
    for(fileStas <- fileStatus) {
      if(fileStas.isDirectory) dirNameList.add(fileStas.getPath.getName)
    }

    println(dirNameList)

    dirNameList

  }

  /**
    * 正则方式列出符合的文件名称含目录
    *
    * /ruoze/temp/emp-1.txt/emp/_SUCCESS
    * /ruoze/temp/emp-1.txt/emp/y=1910/part-00000-491727a0-ea97-4e28-a188-f9addae69a85.c000.csv
    * /ruoze/temp/emp-1.txt/emp/y=1911/part-00000-491727a0-ea97-4e28-a188-f9addae69a85.c000.csv
    *
    * @param path
    * @return
    */
  def listMatchedFileNames(path: String) = {
    val dirNameList = ListBuffer[String]()
    val files = fileSystem.listFiles(new Path(path), true)
    while(files.hasNext) {
      val file = files.next()
      if(file.getPath.getName != "_SUCCESS") {
        dirNameList.add(file.getPath.toUri.getPath)
      }
    }
    dirNameList
  }

  /**
    * 保存文件
    *
    * @param filepath
    */
  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)

    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }

    in.close()
    out.close()
  }

  /**
    * 删除文件
    *
    * @param filename
    * @return
    */
  def deleteFile(filename: String, recursive: Boolean = false): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, recursive)
  }


  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }


  /**
    * 合并目录文件到新目录
    *
    * @param srcPath
    * @param dstPath
    */
  def merge(srcPath: String, dstPath: String): Unit =  {
    println(s"merge ${srcPath} to ${dstPath}")
    // the "true" setting deletes the source files once they are merged into the new output
    FileUtil.copyMerge(fileSystem, new Path(srcPath), fileSystem, new Path(dstPath), true, conf, null)
  }



  def main(args: Array[String]): Unit = {
    //
    //    deleteFile("/data")
    //    deleteFile("/ruoze1")
    //    deleteFile("/ruoze2")
    //    deleteFile("/ruoze4")



    //merge("/ruoze1/emp/y=1982", "/ruoze1/emp/yy=1982/1982.txt")


    //    val parentDirectory = "/ruoze/temp"
    //    val tempList = listMatchedDirectoryNames(s"${parentDirectory}/y=*")
    //    tempList.foreach( temp => {
    //      val targetFile = temp.split("=")(1)
    //      merge(s"${parentDirectory}/${temp}", s"${parentDirectory}/${temp}/${targetFile}")
    //    })

    val parentDirectory = "/ruoze/temp"
    val tempList = listMatchedDirectoryNames("/ruoze/temp/emp-1\\.txt/emp/y=*")
    tempList.foreach( temp => {
      println(temp)

    })


    //    val parentDirectory2 = "/ruoze/temp"
    //    val tempFiles = listMatchedFileNames(s"${parentDirectory2}/emp-1.txt/emp")
    //    tempFiles.foreach(temp => println(temp))


  }

}
