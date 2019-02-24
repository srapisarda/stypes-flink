package uk.ac.bbk.dcs.stypes.flink

import java.io.PrintStream
import java.sql._
import java.util.Properties

import org.apache.calcite.util.Sources
import org.scalatest.{FunSpec, Matchers}


import scala.util.Try

class CalciteSQLTest  extends FunSpec  with Matchers {

  private val model:String = "src/test/resources/benchmark/Lines/data/model"


  it("should be print all tables the model contains") {
    val test = Try {
      val info = new Properties

      info.put("model", jsonPath(model))
      val connection = DriverManager.getConnection("jdbc:calcite:", info)
      val tables: ResultSet = connection.getMetaData.getTables(null, "STYPES", null, null)
      output(tables)

      tables.close()
      connection.close()
    }

    if (test.isFailure)
      fail()
  }

  it("should parse and execute an SQL query using calcite") {
    sql(model, "SELECT * FROM TTLA_ONE a").ok()
  }

  it("should parse and execute an SQL count query using calcite ") {
    sql(model, "SELECT COUNT(*) AS C FROM TTLA_ONE a")
      .returns(List("C=59"))
      .ok()
  }

  it("should parse and execute an SQL count query on TTLR_ONE using calcite ") {
    sql(model, "SELECT COUNT(*) AS C FROM TTLR_ONE a")
      .returns(List("C=61390"))
      .ok()
  }

  it("should parse and execute an SQL count query in empty table using calcite ") {
    sql(model, "SELECT COUNT(*) as C FROM EMPTY_T a")
      .returns(List("C=0"))
      .ok()
  }

  it("should parse and execute an SQL count of join query using calcite ") {
    sql(model, "SELECT COUNT(*) as C " +
      "FROM TTLA_ONE A  " +
      "INNER JOIN TTLR_ONE B ON A.X = B.X " +
      "INNER JOIN EMPTY_T C ON C.X = B.X")
      .returns(List("C=0"))
      .ok()
  }

  it("should parse and execute an SQL plan for count of join query using calcite ") {
    sql(model, "EXPLAIN PLAN FOR SELECT COUNT(*) as C " +
      "FROM TTLA_ONE A  " +
      "INNER JOIN TTLR_ONE B ON A.X = B.X " +
      "INNER JOIN EMPTY_T C ON C.X = B.Y "
    )
      .returns(List("PLAN=EnumerableAggregate(group=[{}], C=[COUNT()])\n" +
        "  EnumerableJoin(condition=[=($0, $2)], joinType=[inner])\n" +
        "    EnumerableInterpreter\n" +
        "      BindableTableScan(table=[[STYPES, TTLA_ONE]])\n" +
        "    EnumerableJoin(condition=[=($0, $2)], joinType=[inner])\n" +
        "      EnumerableInterpreter\n" +
        "        BindableTableScan(table=[[STYPES, EMPTY_T]])\n" +
        "      EnumerableInterpreter\n" +
        "        BindableTableScan(table=[[STYPES, TTLR_ONE]])\n"
      ))
      .ok()
  }

  it("should parse and execute an SQL plan for count of join query using calcite 2") {
    sql(model, "EXPLAIN PLAN FOR SELECT COUNT(*) as NUM " +
      "FROM TTLA_ONE A  " +
      "INNER JOIN TTLR_ONE B1 ON A.X = B1.X " +
      "INNER JOIN TTLR_ONE B2 ON B2.X = B1.X " +
      "INNER JOIN EMPTY_T C1 ON C1.X = B2.Y " +
      "INNER JOIN EMPTY_T C2 ON C2.X = C2.X "
    )
      .ok()
  }


  private def output(resultSet: ResultSet): Unit = {
    try
      output(resultSet, System.out)
    catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    }

  }

  @throws[SQLException]
  private def output(resultSet: ResultSet, out: PrintStream): Unit = {
    //
    val metaData = resultSet.getMetaData
    val columnCount = metaData.getColumnCount

    //
    def outputH(resultSet: ResultSet, out: PrintStream): Unit = {
      if (resultSet.next) {
        for (i <- 1 to columnCount) {
          out.print(resultSet.getString(i))
          if (i < columnCount) out.print(", ")
          else out.println()
        }
        outputH(resultSet, out)
      }
    }
    //
    outputH(resultSet, out)
  }

  private def sql(model: String, sql: String) =
    Fluent(model, sql, output)


  private def checkSql(sql: String, model: String, consumer: ResultSet => Unit  ): Unit = {
    var connection:Option[Connection] = None
    var statement:Option[Statement] = None

    val check = Try {
      val info = new Properties
      info.put("model", jsonPath(model))
      connection = Some(DriverManager.getConnection("jdbc:calcite:", info))
      statement = Some(connection.get.createStatement())
      val resultSet = statement.get.executeQuery(sql)
      consumer.apply(resultSet)

    }
    // close the connection
    close(connection, statement)

    if (check.isFailure)
      throw check.failed.get

  }

  private def close(connection: Option[Connection], statement: Option[Statement]): Unit = {
    Try(if (statement.isDefined) statement.get.close())
    Try(if (connection.isDefined) connection.get.close())
  }

  private def jsonPath(model: String) =
    resourcePath(model + ".json")

  private def resourcePath(path: String) =
    Sources.of(new java.io.File(path)).file.getAbsolutePath


  /** Returns a function that checks the contents of a result set against an
    * expected string. */
  private def checkExpected(expected: List[String]): ResultSet => Unit =
    (resultSet: ResultSet) => {
      try {
        val lines: List[String] = collect(resultSet)
        expected should equal (lines)
      } catch {
        case e: SQLException =>
          throw new RuntimeException(e)
      }

    }

  @throws[SQLException]
  private def collect(resultSet: ResultSet, acc: List[String] = List()): List[String] = {
    val buf = new StringBuilder
    if (resultSet.next()) {
      buf.setLength(0)
      val n = resultSet.getMetaData.getColumnCount
      var sep = ""
      for (i <- 1 to n) {
        buf
          .append(sep)
          .append(resultSet.getMetaData.getColumnLabel(i))
          .append("=")
          .append(resultSet.getString(i))
        sep = "; "

      }
      collect(resultSet, buf.mkString :: acc)
    } else acc.reverse
  }


  private case class Fluent(model: String, sql: String, expect: ResultSet => Unit) {
    /** Runs the test. */
    def ok(): Fluent = {
      val test = Try(checkSql(sql, model, expect))
      if (test.isSuccess) this
      else throw test.failed.get
    }

    def checking(expect: ResultSet => Unit):Fluent =
      Fluent(model, sql, expect)

    /** Sets the rows that are expected to be returned from the SQL query. */

    def  returns (expectedLines: List[String]): Fluent=
      checking(checkExpected(expectedLines))
  }

}

