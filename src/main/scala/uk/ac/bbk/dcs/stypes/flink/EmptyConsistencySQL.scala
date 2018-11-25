package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.flink.table.api.TableEnvironment

/**
  * Created by salvo on 19/11/2018.
  */
object EmptyConsistencySQL extends BaseFlinkRewriting {
  private val tableEnv = TableEnvironment.getTableEnvironment(env)

  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      EmptyConsistency.run(args(0).toInt, args(1))
    else
      EmptyConsistency.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    tableEnv.registerTableSource("R", getDataSourceR(fileNumber))
    tableEnv.registerTableSource("S", getDataSourceR(fileNumber))


    executeAsTable(fileNumber, serial, "empty-consistency", _ => {
      val r = tableEnv.scan("R")
      val s = tableEnv.scan("S")
      r.join(r, ""  ). join(r)
    } )

  }
}