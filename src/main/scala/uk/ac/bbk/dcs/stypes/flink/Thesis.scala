package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.table.api.{Table, TableEnvironment}

import java.util.UUID
import scala.util.Try

//uk.ac.bbk.dcs.stypes.flink.Thesis
object Thesis extends BaseFlinkTableRewritingT {

  val DEFAULT_TTL_FILE_NUMBER = 1

  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 2) {
      Thesis.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(true))
    } else if (args.length > 1) {
      Thesis.run(fileNumber, args(1))
    } else {
      Thesis.run(fileNumber)
    }
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString,
          enableOptimisation: Boolean = true, sql: String = ""): Unit = {

    def tableRewritingEvaluation(fileNumber: Int, jobName: String, tableEnv: TableEnvironment): Table = {
      println(s"sql argument: $sql")
      lazy val table = tableEnv.sqlQuery(sql)
      table
    }

    val jobName = s"thesis-example-01_${env.getParallelism}-$serial"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName, enableOptimisation)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }

}
