package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.table.api.{Table, TableEnvironment}

import java.util.UUID
import scala.util.Try

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSqlQ15With
object FlinkRewritingSqlQ15With extends BaseFlinkTableRewriting {
    val DEFAULT_TTL_FILE_NUMBER = 3

    def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
      val jobName = "sql-q22-with-ex"
      val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
      executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
    }

    private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
      lazy val p1 = tableEnv.sqlQuery(
        """WITH p28 AS (SELECT S_0.X AS X0, R_1.Y AS X1
          |             FROM S AS S_0
          |                      INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |                      INNER JOIN A AS A_2 ON R_1.Y = A_2.X
          |             UNION
          |             (SELECT A_0.X AS X0, A_0.X AS X1 FROM A AS A_0)),
          |     p7 AS (SELECT S_0.X AS X0, R_1.Y AS X1
          |            FROM S AS S_0
          |                     INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |            UNION
          |            (SELECT A_0.X AS X0, A_0.X AS X1 FROM A AS A_0)),
          |     p14 AS (SELECT R_0.X AS X0, p7_2.X1 AS X1
          |             FROM R AS R_0
          |                      INNER JOIN S AS S_1 ON R_0.Y = S_1.X
          |                      INNER JOIN p7 AS p7_2 ON S_1.Y = p7_2.X0
          |             UNION
          |             (SELECT B_0.X AS X0, p7_1.X1 AS X1
          |              FROM B AS B_0
          |                       INNER JOIN p7 AS p7_1 ON B_0.X = p7_1.X0)),
          |     p5 AS (SELECT B_0.X AS X0, B_0.X AS X1
          |            FROM B AS B_0
          |            UNION
          |            (SELECT S_0.Y AS X0, R_1.X AS X1
          |             FROM S AS S_0
          |                      INNER JOIN R AS R_1 ON S_0.X = R_1.Y)),
          |     p2 AS (SELECT p5_1.X1 AS X0, p14_2.X1 AS X1
          |            FROM R AS R_0
          |                     INNER JOIN p5 AS p5_1 ON R_0.X = p5_1.X0
          |                     INNER JOIN p14 AS p14_2 ON R_0.Y = p14_2.X0
          |            UNION
          |            (SELECT R_1.X AS X0, p14_0.X1 AS X1
          |             FROM p14 AS p14_0
          |                      INNER JOIN R AS R_1 ON p14_0.X0 = R_1.Y
          |                      INNER JOIN A AS A_2 ON p14_0.X0 = A_2.X)),
          |     p43 AS (SELECT S_2.Y AS X0, S_0.X AS X1
          |             FROM S AS S_0
          |                      INNER JOIN R AS R_1 ON S_0.Y = R_1.X
          |                      INNER JOIN S AS S_2 ON R_1.Y = S_2.X
          |             UNION
          |             (SELECT S_1.Y AS X0, A_0.X AS X1
          |              FROM A AS A_0
          |                       INNER JOIN S AS S_1 ON A_0.X = S_1.X)
          |             UNION
          |             (SELECT S_0.Y AS X0, S_0.X AS X1
          |              FROM S AS S_0
          |                       INNER JOIN B AS B_1 ON S_0.Y = B_1.X)),
          |     p40 AS (SELECT S_2.Y AS X0, B_0.X AS X1
          |             FROM B AS B_0
          |                      INNER JOIN R AS R_1 ON B_0.X = R_1.X
          |                      INNER JOIN S AS S_2 ON R_1.Y = S_2.X
          |             UNION
          |             (SELECT B_0.X AS X0, B_0.X AS X1 FROM B AS B_0)),
          |     p19 AS (SELECT R_0.X AS X0, R_0.Y AS X1
          |             FROM R AS R_0
          |                      INNER JOIN B AS B_1 ON R_0.Y = B_1.X
          |             UNION
          |             (SELECT R_0.X AS X0, S_2.Y AS X1
          |              FROM R AS R_0
          |                       INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                       INNER JOIN S AS S_2 ON R_1.Y = S_2.X)),
          |     p3 AS (SELECT R_0.X AS X0, p28_2.X1 AS X1
          |            FROM R AS R_0
          |                     INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                     INNER JOIN p28 AS p28_2 ON R_1.Y = p28_2.X0
          |                     INNER JOIN A AS A_3 ON R_1.Y = A_3.X
          |            UNION
          |            (SELECT p19_0.X0 AS X0, p28_2.X1 AS X1
          |             FROM p19 AS p19_0
          |                      INNER JOIN R AS R_1 ON p19_0.X1 = R_1.X
          |                      INNER JOIN p28 AS p28_2 ON R_1.Y = p28_2.X0)
          |            UNION
          |            (SELECT p19_0.X0 AS X0, R_2.Y AS X1
          |             FROM p19 AS p19_0
          |                      INNER JOIN B AS B_1 ON p19_0.X1 = B_1.X
          |                      INNER JOIN R AS R_2 ON p19_0.X1 = R_2.X
          |                      INNER JOIN A AS A_3 ON R_2.Y = A_3.X)),
          |     p35 AS (SELECT p19_0.X0 AS X0, p40_1.X0 AS X1
          |             FROM p19 AS p19_0
          |                      INNER JOIN p40 AS p40_1 ON p19_0.X1 = p40_1.X1
          |                      INNER JOIN B AS B_2 ON p19_0.X1 = B_2.X
          |             UNION
          |             (SELECT p19_0.X0 AS X0, p43_2.X0 AS X1
          |              FROM p19 AS p19_0
          |                       INNER JOIN R AS R_1 ON p19_0.X1 = R_1.X
          |                       INNER JOIN p43 AS p43_2 ON R_1.Y = p43_2.X1)
          |             UNION
          |             (SELECT R_0.X AS X0, p43_2.X0 AS X1
          |              FROM R AS R_0
          |                       INNER JOIN R AS R_1 ON R_0.Y = R_1.X
          |                       INNER JOIN p43 AS p43_2 ON R_1.Y = p43_2.X1
          |                       INNER JOIN A AS A_3 ON R_1.Y = A_3.X)),
          |     p1 AS (SELECT p35_0.X0 AS X0, p2_2.X1 AS X1
          |            FROM p35 AS p35_0
          |                     INNER JOIN R AS R_1 ON p35_0.X1 = R_1.X
          |                     INNER JOIN p2 AS p2_2 ON R_1.Y = p2_2.X0
          |            UNION
          |            (SELECT p35_0.X0 AS X0, S_1.X AS X1
          |             FROM p35 AS p35_0
          |                      INNER JOIN S AS S_1 ON p35_0.X1 = S_1.Y
          |                      INNER JOIN B AS B_2 ON p35_0.X1 = B_2.X)
          |            UNION
          |            (SELECT p3_0.X0 AS X0, p2_1.X1 AS X1
          |             FROM p3 AS p3_0
          |                      INNER JOIN p2 AS p2_1 ON p3_0.X1 = p2_1.X0
          |                      INNER JOIN A AS A_2 ON p3_0.X1 = A_2.X))
          |SELECT DISTINCT p1.X0 AS x, p1.X1 AS y
          |FROM p1""".stripMargin)
      p1
    }

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      FlinkRewritingSqlQ15With.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false))
    } else if (args.length > 1) {
      FlinkRewritingSqlQ15With.run(fileNumber, args(1))
    } else {
      FlinkRewritingSqlQ15With.run(fileNumber)
    }
  }
}
