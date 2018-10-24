package main.scala.com.jdbc
import scalikejdbc._


/**
  * @author PGOne
  * @ date 2018/10/24
  */
object DBInitializer {

  def run() {
    DB readOnly { implicit s =>
      try {
        sql"select 1 from user limit 1".map(_.long(1)).single.apply()
      } catch {
        case e: java.sql.SQLException =>
          DB autoCommit { implicit s =>
            sql"""
              create table user (
                id bigint auto_increment primary key,
                name varchar(255) not null,
                age int default 0,
                address varchar(255)
              );
            """.execute.apply()
          }
      }
    }
  }

}

