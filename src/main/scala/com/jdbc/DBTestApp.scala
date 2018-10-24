package main.scala.com.jdbc

import scalikejdbc.config.DBs

/**
  * @author PGOne
  * @date 2018/10/24
  */
object DBTestApp {

  def main(args: Array[String]): Unit = {

    DBs.setupAll

    DBInitializer.run() //初始化:建表

    //插入用户
    var createdUser = User.create("Lois", 18, Some("USA"))
    println(s"新增用户成功. id:${createdUser.id} name:${createdUser.name}, age: ${createdUser.age}")

    //查询用户
    val user = User.find(createdUser.id)

    //修改用户年龄
    User.save(User(createdUser.id, createdUser.name, createdUser.age + 100, createdUser.address))

    //查询所有用户
    val allResults = User.findAll()
    allResults.foreach(result => println(s"name:${result.name}, age:${result.age}, address:${result.address}"))

    //删除所有用户
    val entities = User.findAll()
    entities.foreach(e => User.destroy(e.id))

    DBs.closeAll

  }

}

