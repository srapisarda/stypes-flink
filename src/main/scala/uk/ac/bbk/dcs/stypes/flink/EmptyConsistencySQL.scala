package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
  * Created by salvo on 19/11/2018.
  */
object EmptyConsistencySQL extends BaseFlinkRewriting {
  private val tableEnv = BatchTableEnvironment.create(env)

  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      EmptyConsistencySQL.run(args(0).toInt, args(1))
    else
      EmptyConsistencySQL.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    val rDataSource: CsvTableSource = getDataSourceR(fileNumber)
    tableEnv.registerTableSource("R", rDataSource)
    tableEnv.registerTableSource("S", getDataSourceR(fileNumber))


    executeAsTable(fileNumber, serial, "empty-consistency", _ => {
      val r: Table = tableEnv.from("R")
      val s = tableEnv.from("S")
      r.join(r, ""  ). join(r)
    } )

  }
}