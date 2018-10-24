
package main.scala.com.jdbc
import scalikejdbc._
/**
  * @author PGOne
  * @ date 2018/10/24
  */
case class User(
                 id: Long,
                 name: String,
                 age: Int,
                 address: Option[String]) {

  def save()(implicit session: DBSession = User.autoSession): User = User.save(this)(session)
  def destroy()(implicit session: DBSession = User.autoSession): Unit = User.destroy(id)(session)

}

/**
  *
  */
object User extends SQLSyntaxSupport[User] {

  def apply(c: SyntaxProvider[User])(rs: WrappedResultSet): User = apply(c.resultName)(rs)

  def apply(c: ResultName[User])(rs: WrappedResultSet): User = new User(
    id = rs.get(c.id),
    name = rs.get(c.name),
    age = rs.get(c.age),
    address = rs.get(c.address))

  val c = User.syntax("c")

  def find(id: Long)(implicit session: DBSession = autoSession): Option[User] = withSQL {
    select.from(User as c).where.eq(c.id, id)
  }.map(User(c)).single.apply()

  def findAll()(implicit session: DBSession = autoSession): List[User] = withSQL {
    select.from(User as c).orderBy(c.id)
  }.map(User(c)).list.apply()

  def countAll()(implicit session: DBSession = autoSession): Long = withSQL {
    select(sqls.count).from(User as c)
  }.map(rs => rs.long(1)).single.apply().get

  def findAllBy(where: SQLSyntax)(implicit session: DBSession = autoSession): List[User] = withSQL {
    select.from(User as c)
      .where.append(sqls"${where}")
      .orderBy(c.id)
  }.map(User(c)).list.apply()

  def countBy(where: SQLSyntax)(implicit session: DBSession = autoSession): Long = withSQL {
    select(sqls.count).from(User as c).where.append(sqls"${where}")
  }.map(_.long(1)).single.apply().get

  def create(
              name: String,
              age: Int,
              address: Option[String] = None)(implicit session: DBSession = autoSession): User = {

    val id = withSQL {
      insert.into(User).namedValues(
        column.name -> name,
        column.age -> age,
        column.address -> address)
    }.updateAndReturnGeneratedKey.apply()

    User(
      id = id,
      name = name,
      age = age,
      address = address)
  }

  def save(m: User)(implicit session: DBSession = autoSession): User = {
    withSQL {
      update(User).set(
        column.name -> m.name,
        column.age -> m.age,
        column.address -> m.address).where.eq(column.id, m.id)
    }.update.apply()
    m
  }

  def destroy(id: Long)(implicit session: DBSession = autoSession): Unit = withSQL {
    delete.from(User).where.eq(column.id, id)
  }.update.apply()

}

