package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.table.api.{Table, TableEnvironment}

import java.util.UUID
import scala.util.Try

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSqlQ45With
object FlinkRewritingSqlQ45With extends BaseFlinkTableRewriting {
    val DEFAULT_TTL_FILE_NUMBER = 3

    def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
      val jobName = "sql-q45-with-ex"
      val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
      executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
    }

    private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
      lazy val p1 = tableEnv.sqlQuery(
        """WITH p41 AS (SELECT A_0.X AS X0, A_0.X AS X1
          |             FROM A AS A_0
          |             UNION
          |             (SELECT S_0.X AS X0, R_1.Y AS X1
          |              FROM S AS S_0
          |                       INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |                       INNER JOIN A AS A_2 ON R_1.Y = A_2.X)),
          |     p45 AS (SELECT S_2.X AS X0, B_0.X AS X1
          |             FROM B AS B_0
          |                      INNER JOIN R AS R_1 ON B_0.X = R_1.Y
          |                      INNER JOIN S AS S_2 ON R_1.X = S_2.Y
          |             UNION
          |             (SELECT A_0.X AS X0, A_0.X AS X1
          |              FROM A AS A_0
          |                       INNER JOIN B AS B_1 ON A_0.X = B_1.X)),
          |     p42 AS (SELECT R_0.Y AS X0, R_0.X AS X1
          |             FROM R AS R_0
          |                      INNER JOIN A AS A_1 ON R_0.X = A_1.X
          |             UNION
          |             (SELECT R_1.Y AS X0, S_2.X AS X1
          |              FROM R AS R_0
          |                       INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                       INNER JOIN S AS S_2 ON R_0.X = S_2.Y)),
          |     p21 AS (SELECT S_2.Y AS X0, A_0.X AS X1
          |             FROM A AS A_0
          |                      INNER JOIN R AS R_1 ON A_0.X = R_1.X
          |                      INNER JOIN S AS S_2 ON R_1.Y = S_2.X
          |             UNION
          |             (SELECT A_0.X AS X0, A_0.X AS X1
          |              FROM A AS A_0
          |                       INNER JOIN B AS B_1 ON A_0.X = B_1.X)),
          |     p31 AS (SELECT R_0.X AS X0, S_2.Y AS X1
          |             FROM R AS R_0
          |                      INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                      INNER JOIN S AS S_2 ON R_1.Y = S_2.X
          |             UNION
          |             (SELECT R_0.X AS X0, R_0.Y AS X1
          |              FROM R AS R_0
          |                       INNER JOIN B AS B_1 ON R_0.Y = B_1.X)),
          |     p37 AS (SELECT p42_1.X1 AS X0, p21_2.X0 AS X1
          |             FROM A AS A_0
          |                      INNER JOIN p42 AS p42_1 ON A_0.X = p42_1.X0
          |                      INNER JOIN p21 AS p21_2 ON A_0.X = p21_2.X1
          |             UNION
          |             (SELECT p45_1.X0 AS X0, p31_2.X1 AS X1
          |              FROM B AS B_0
          |                       INNER JOIN p45 AS p45_1 ON B_0.X = p45_1.X1
          |                       INNER JOIN p31 AS p31_2 ON B_0.X = p31_2.X0)
          |             UNION
          |             (SELECT p42_0.X1 AS X0, p31_2.X1 AS X1
          |              FROM p42 AS p42_0
          |                       INNER JOIN S AS S_1 ON p42_0.X0 = S_1.X
          |                       INNER JOIN p31 AS p31_2 ON S_1.Y = p31_2.X0)),
          |     p3 AS (SELECT p31_2.X1 AS X0, R_1.X AS X1
          |            FROM B AS B_0
          |                     INNER JOIN R AS R_1 ON B_0.X = R_1.Y
          |                     INNER JOIN p31 AS p31_2 ON B_0.X = p31_2.X0
          |                     INNER JOIN B AS B_3 ON R_1.X = B_3.X
          |            UNION
          |            (SELECT p21_3.X0 AS X0, R_0.X AS X1
          |             FROM R AS R_0
          |                      INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                      INNER JOIN A AS A_2 ON R_1.Y = A_2.X
          |                      INNER JOIN p21 AS p21_3 ON R_1.Y = p21_3.X1
          |                      INNER JOIN B AS B_4 ON R_0.X = B_4.X)
          |            UNION
          |            (SELECT p31_3.X1 AS X0, R_0.X AS X1
          |             FROM R AS R_0
          |                      INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                      INNER JOIN S AS S_2 ON R_1.Y = S_2.X
          |                      INNER JOIN p31 AS p31_3 ON S_2.Y = p31_3.X0
          |                      INNER JOIN B AS B_4 ON R_0.X = B_4.X)),
          |     p5 AS (SELECT A_0.X AS X0, R_1.Y AS X1
          |            FROM A AS A_0
          |                     INNER JOIN R AS R_1 ON A_0.X = R_1.X
          |            UNION
          |            (SELECT S_0.X AS X0, R_2.Y AS X1
          |             FROM S AS S_0
          |                      INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |                      INNER JOIN R AS R_2 ON R_1.Y = R_2.X)),
          |     p14 AS (SELECT S_1.Y AS X0, A_0.X AS X1
          |             FROM A AS A_0
          |                      INNER JOIN S AS S_1 ON A_0.X = S_1.X
          |             UNION
          |             (SELECT S_2.Y AS X0, S_0.X AS X1
          |              FROM S AS S_0
          |                       INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |                       INNER JOIN S AS S_2 ON R_1.Y = S_2.X)
          |             UNION
          |             (SELECT S_0.Y AS X0, S_0.X AS X1
          |              FROM S AS S_0
          |                       INNER JOIN B AS B_1 ON S_0.Y = B_1.X)),
          |     p15 AS (SELECT B_0.X AS X0, B_0.X AS X1
          |             FROM B AS B_0
          |                      INNER JOIN A AS A_1 ON B_0.X = A_1.X
          |             UNION
          |             (SELECT S_0.X AS X0, R_1.Y AS X1
          |              FROM S AS S_0
          |                       INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |                       INNER JOIN B AS B_2 ON R_1.Y = B_2.X)),
          |     p36 AS (SELECT p5_0.X0 AS X0, p41_2.X1 AS X1
          |             FROM p5 AS p5_0
          |                      INNER JOIN S AS S_1 ON p5_0.X1 = S_1.X
          |                      INNER JOIN p41 AS p41_2 ON S_1.Y = p41_2.X0
          |             UNION
          |             (SELECT p15_0.X0 AS X0, p41_2.X1 AS X1
          |              FROM p15 AS p15_0
          |                       INNER JOIN B AS B_1 ON p15_0.X1 = B_1.X
          |                       INNER JOIN p41 AS p41_2 ON p15_0.X1 = p41_2.X0)),
          |     p2 AS (SELECT p15_0.X0 AS X0, p14_2.X0 AS X1
          |            FROM p15 AS p15_0
          |                     INNER JOIN B AS B_1 ON p15_0.X1 = B_1.X
          |                     INNER JOIN p14 AS p14_2 ON p15_0.X1 = p14_2.X1
          |            UNION
          |            (SELECT p5_0.X0 AS X0, p14_2.X0 AS X1
          |             FROM p5 AS p5_0
          |                      INNER JOIN S AS S_1 ON p5_0.X1 = S_1.X
          |                      INNER JOIN p14 AS p14_2 ON S_1.Y = p14_2.X1)),
          |     p1 AS (SELECT p2_0.X0 AS X0, p3_2.X0 AS X1
          |            FROM p2 AS p2_0
          |                     INNER JOIN B AS B_1 ON p2_0.X1 = B_1.X
          |                     INNER JOIN p3 AS p3_2 ON p2_0.X1 = p3_2.X1
          |            UNION
          |            (SELECT p2_0.X0 AS X0, p37_2.X1 AS X1
          |             FROM p2 AS p2_0
          |                      INNER JOIN R AS R_1 ON p2_0.X1 = R_1.X
          |                      INNER JOIN p37 AS p37_2 ON R_1.Y = p37_2.X0)
          |            UNION
          |            (SELECT p36_0.X0 AS X0, p37_2.X1 AS X1
          |             FROM p36 AS p36_0
          |                      INNER JOIN A AS A_1 ON p36_0.X1 = A_1.X
          |                      INNER JOIN p37 AS p37_2 ON p36_0.X1 = p37_2.X0))
          |SELECT DISTINCT p1.X0 AS x, p1.X1 AS y
          |FROM p1""".stripMargin)
      p1
    }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSqlQ45With.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1) {
      FlinkRewritingSqlQ45With.run(fileNumber, args(1))
    } else {
      FlinkRewritingSqlQ45With.run(fileNumber)
    }
  }
}
