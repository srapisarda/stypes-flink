package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._

import scala.util.Try

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql01
object FlinkRewritingSql01 extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    val jobName = "sql-q01-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    val a = tableEnv.sqlQuery("select X as a_x, X as a_y from A")
    val r = tableEnv.sqlQuery("select X as r_x, Y as r_y from R")

    // p1(x12,x7) :- r(x7,x8), a(x8), r(x8,x11), r(x11,x12).
    val p1 = r
      .join(a).where("r_y=a_x").select("r_x as x7, a_x as x8")
      .join(r).where("x8=r_x").select("x7, r_y as x11")
      .join(r).where("x11=r_x").select("r_y as x, x7 as y")

    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 1)
      FlinkRewritingSql01.run(fileNumber, args(1))
    else {
      FlinkRewritingSql01.run(fileNumber)
    }
  }
}

//uk.ac.bbk.dcs.stypes.flink.FlinkRewriting01
object FlinkRewriting01 extends BaseFlinkRewriting {
  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting02.run(args(0).toInt, args(1))
    else
      FlinkRewriting02.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q01-ex", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {
    val a: DataSet[(String, String)] = getA(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)

    // p1(x12,x7) :- r(x7,x8), a(x8), r(x8,x11), r(x11,x12).
    val p1 = r
      .join(a).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._2._2, p._1._2))

    p1
  }
}


//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql02
object FlinkRewritingSql02 extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    val jobName = "sql-q02-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    val a: Table = tableEnv.sqlQuery("select X as a_x, X as a_y from A")
    val b = tableEnv.sqlQuery("select X as b_x, X as b_y from B")
    val s = tableEnv.sqlQuery("select X as s_x, Y as s_y from S")
    val r = tableEnv.sqlQuery("select X as r_x, Y as r_y from R")

    // p1(x11,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11).
    val p1 = r
      .join(a).where("r_y=a_x").select("r_x as x7, a_x as x8")
      .join(s).where("x8=s_x").select("x7, s_y as x11")
      .join(b).where("x11=b_x").select("b_x as x, x7 as y")

    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 1)
      FlinkRewritingSql02.run(fileNumber, args(1))
    else {
      FlinkRewritingSql02.run(fileNumber)
    }
  }
}

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql04
object FlinkRewritingSql04 extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
    val jobName = "sql-q04-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName, enableOptimisation)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    // p1(x11,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11).
    val p1 = tableEnv.sqlQuery("select R.X as x, B.X as y from R " +
      "inner join A on R.Y = A.X " +
      "inner join S on A.X = S.X " +
      "inner join B on S.Y = B.X")
    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSql04.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1)
      FlinkRewritingSql04.run(fileNumber, args(1))
    else {
      FlinkRewritingSql04.run(fileNumber)
    }
  }
}


//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql05
object FlinkRewritingSql05 extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
    val jobName = "sql-q05-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName, enableOptimisation)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    // p1(x11,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11).
    val p1 = tableEnv.sqlQuery("select R.X as x, B.X as y from R " +
      "inner join A on R.Y = A.X " +
      "inner join T on A.X = T.X " +
      "inner join B on T.Y = B.X")
    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSql05.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1)
      FlinkRewritingSql05.run(fileNumber, args(1))
    else {
      FlinkRewritingSql05.run(fileNumber)
    }
  }
}

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql05With
object  FlinkRewritingSql05With extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
    val jobName = "sql-q05-ex-with"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName, enableOptimisation)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    // p1(x11,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11).
    val p1 = tableEnv.sqlQuery("with p1 as (select R.X as x, B.X as y from R " +
      "inner join A on R.Y = A.X " +
      "inner join S on A.X = S.X " +
      "inner join B on S.Y = B.X) select p1.x, p1.y from p1")
    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSql05With.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1)
      FlinkRewritingSql05With.run(fileNumber, args(1))
    else {
      FlinkRewritingSql05With.run(fileNumber)
    }
  }
}


//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSqlQ22
object FlinkRewritingSqlQ22 extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
    val jobName = "sql-q22-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    // p1(x0,x7) :- p3(x0,x3), r(x3,x4), p12(x7,x4).
    // p3(x0,x3) :-  a(x0), r(x0,x3).
    // p3(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3).
    // p12(x7,x4) :-  r(x4,x5), r(x5,x6), s(x6,x7).
    // p12(x7,x4) :- r(x4,x7), b(x7).

    lazy val p1 = tableEnv.sqlQuery(
      """|select distinct p3.X as x, p12.X as y from
         |(
         |select A.X, R.Y from A inner join R on A.X = R.X
         |union
         |select S.X, R2.Y from S
         |inner join R as R1 on S.Y = R1.X
         |inner join R as R2 on R1.Y = R2.X
         |) as p3
         |inner join R on p3.Y = R.X
         |inner join
         |(
         |select S.Y as X, R1.X as Y from
         |R as R1 inner join R as R2 on R1.Y = R2.X
         |inner join S on R2.Y = S.X
         |union
         |select B.X as X, R.X as Y
         |from R inner join B on R.Y = B.X
         |) as p12
         |on R.Y = p12.Y
         |""".stripMargin)

    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSqlQ22.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1) {
      FlinkRewritingSqlQ22.run(fileNumber, args(1))
    } else {
      FlinkRewritingSqlQ22.run(fileNumber)
    }
  }
}

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSqlQ22With
object FlinkRewritingSqlQ22With extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
    val jobName = "sql-q22-with-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    // p1(x0,x7) :- p3(x0,x3), r(x3,x4), p12(x7,x4).
    // p3(x0,x3) :-  a(x0), r(x0,x3).
    // p3(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3).
    // p12(x7,x4) :-  r(x4,x5), r(x5,x6), s(x6,x7).
    // p12(x7,x4) :- r(x4,x7), b(x7).

    lazy val p1 = tableEnv.sqlQuery(
      """WITH p12 AS (SELECT S_2.Y AS X0, R_0.X AS X1
        |             FROM R AS R_0
        |                      INNER JOIN R AS R_1 ON R_0.Y = R_1.X
        |                      INNER JOIN S AS S_2 ON R_1.Y = S_2.X
        |             UNION
        |             (SELECT R_0.Y AS X0, R_0.X
        |              AS X1
        |              FROM R AS R_0
        |                       INNER JOIN B AS B_1 ON R_0.Y = B_1.X)),
        |     p3 AS (SELECT A_0.X AS X0, R_1.Y AS X1
        |            FROM A AS A_0
        |                     INNER JOIN R AS R_1 ON A_0.X = R_1.X
        |            UNION
        |            (SELECT S_0.X AS X0, R_2.Y AS X1
        |             FROM S AS S_0
        |                      INNER JOIN R AS R_1 ON S_0.Y = R_1.X
        |                      INNER JOIN R AS R_2 ON R_1.Y = R_2.X)),
        |     p1 AS (SELECT p3_0.X0 AS X0, p12_2.X0 AS X1
        |            FROM p3 AS p3_0
        |                     INNER JOIN R AS R_1 ON p3_0.X1 = R_1.X
        |                     INNER JOIN p12 AS p12_2 ON R_1.Y = p12_2.X1)
        |SELECT DISTINCT p1.X0 as x, p1.X1 as y
        |FROM p1""".stripMargin)

    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSqlQ22With.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1) {
      FlinkRewritingSqlQ22With.run(fileNumber, args(1))
    } else {
      FlinkRewritingSqlQ22With.run(fileNumber)
    }
  }
}


//uk.ac.bbk.dcs.stypes.flink.FlinkRewriting02
object FlinkRewriting02 extends BaseFlinkRewriting {
  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting02.run(args(0).toInt, args(1))
    else
      FlinkRewriting02.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q01-ex", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {
    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)


    // p1(x11,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11).
    val p1 = r
      .join(a).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(s).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(b).where(1).equalTo(0).map(p => (p._2._1, p._1._2))

    p1
  }
}


//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql03
object FlinkRewritingSql03 extends BaseFlinkTableRewriting {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    val jobName = "sql-q02-ex"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

  private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
    val a = tableEnv.sqlQuery("select X as a_x, X as a_y from A")
    val b = tableEnv.sqlQuery("select X as b_x, X as b_y from B")
    val s = tableEnv.sqlQuery("select X as s_x, Y as s_y from S")
    val r = tableEnv.sqlQuery("select X as r_x, Y as r_y from R")

    // p1(x12,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11), r(x11, x12).
    val p1 = r
      .join(a).where("r_y=a_x").select("r_x as x7, a_x as x8")
      .join(s).where("x8=s_x").select("x7, s_y as x11")
      .join(b).where("x11=b_x").select("x7, b_x")
      .join(r).where("b_x=r_x").select("r_y as x, x7 as y")

    p1
  }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 1)
      FlinkRewritingSql03.run(fileNumber, args(1))
    else {
      FlinkRewritingSql03.run(fileNumber)
    }
  }
}

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingEx
object FlinkRewritingEx extends BaseFlinkRewriting {


  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewritingEx.run(args(0).toInt, args(1))
    else
      FlinkRewritingEx.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "EX", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)


    // p1(x12,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11), r(x11,x12).
    val p1_0 = r.join(r).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(s).where(1).equalTo(0).map(p => (p._1._2, p._2._2))

    val p1_1 = r.join(b).where(1).equalTo(0).map(p => (p._1._2, p._2._2))

    val p1 = p1_0.union(p1_1)

    p1
  }
}


//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingEx2
object FlinkRewritingEx2 extends BaseFlinkRewriting {


  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewritingEx2.run(args(0).toInt, args(1))
    else
      FlinkRewritingEx2.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "EX", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)


    // p1(x12,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11), r(x11,x12).
    val p1_0 = s.join(r).where(0).equalTo(1).map(p => (p._2._1, p._1._2))
      .join(r).where(0).equalTo(1).map(p => (p._2._1, p._1._2))

    val p1_1 = r.join(b).where(1).equalTo(0).map(p => (p._1._2, p._2._2))

    val p1 = p1_0.union(p1_1)

    p1
  }
}

//uk.ac.bbk.dcs.stypes.flink.FlinkRewriting03
object FlinkRewriting03 extends BaseFlinkRewriting {
  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting03.run(args(0).toInt, args(1))
    else
      FlinkRewriting03.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q01-ex", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {
    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)


    // p1(x12,x7) :- r(x7,x8), a(x8), s(x8,x11), b(x11), r(x11,x12).
    val p1 = r
      .join(a).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(s).where(1).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(b).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._2._2, p._1._1))
    p1
  }
}