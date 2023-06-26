package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.table.api.{Table, TableEnvironment}


import java.util.UUID
import scala.util.Try

object Thesis {
  //uk.ac.bbk.dcs.stypes.flink.ThesisExample01
  object ThesisExample01 extends BaseFlinkTableRewritingT {
    val DEFAULT_TTL_FILE_NUMBER = 1

    def run(fileNumber: Int, serial: String = UUID.randomUUID().toString, enableOptimisation: Boolean = true): Unit = {
      val jobName = "thesis-example-01"
      val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName, enableOptimisation)
      executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
    }

    private def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
      lazy val p1 = tableEnv.sqlQuery(
        """WITH p AS (
          | SELECT employee_id AS y, project_id AS z FROM employee_project
          | UNION
          | SELECT employee_id AS y, project_id  AS z
          |   FROM employee_project AS ep
          |   INNER JOIN employee AS e ON ep.employee_id = e.id
          |   WHERE e.manager_id IS NOT NULL
          |)
          |SELECT persName1.name AS x1, persName2.name AS x2
          |FROM employee AS persName1
          |INNER JOIN p AS p1 ON p1.y = persName1.id
          |INNER JOIN p AS p2 ON  p1.z = p2.z
          |INNER JOIN employee AS persName2 ON p2.y = persName2.id""".stripMargin)

      p1
    }

    def main(args: Array[String]): Unit = {
      val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
      if (args.length > 2) {
        ThesisExample01.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(true))
      } else if (args.length > 1) {
        ThesisExample01.run(fileNumber, args(1))
      } else {
        ThesisExample01.run(fileNumber)
      }
    }
  }

}
