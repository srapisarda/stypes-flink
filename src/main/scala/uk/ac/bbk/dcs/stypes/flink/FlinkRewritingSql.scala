package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.table.api.{Table, TableEnvironment}

import java.util.UUID
import scala.util.Try

//uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql
object FlinkRewritingSql extends BaseFlinkTableRewritingLC {
  val DEFAULT_TTL_FILE_NUMBER = 3

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString,
          enableOptimisation: Boolean = true, sql:String = "" ): Unit = {

    def tableRewritingEvaluation(fileNumber: Int, jobName: String,  tableEnv: TableEnvironment): Table = {
      lazy val p1 = tableEnv.sqlQuery(sql)
      p1
    }

    val jobName = s"p_${env.getParallelism}-$serial"
    val tableEnv: TableEnvironment = makeTableEnvironment(fileNumber, jobName)
    executeTableRewriting(fileNumber, serial, jobName, tableEnv, tableRewritingEvaluation)
  }



  def main(args: Array[String]): Unit = {
    val fileNumber = if (args.isEmpty) DEFAULT_TTL_FILE_NUMBER else args(0).toInt
    if (args.length > 3) {
      FlinkRewritingSql.run(fileNumber, args(1), Try(args(2).toBoolean).getOrElse(false), args(3) )
    } else {
      throw new RuntimeException("Too low parameters. Please provide ")
    }
  }
}
