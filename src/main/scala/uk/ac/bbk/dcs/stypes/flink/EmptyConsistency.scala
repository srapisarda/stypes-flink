package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.flink.api.scala.DataSet
/**
  * Created by salvo on 19/11/2018.
  */
object EmptyConsistency extends BaseFlinkRewriting {
  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting4q15.run(args(0).toInt, args(1))
    else
      FlinkRewriting4q15.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)

    execute(fileNumber, serial, "empty-consistency", (_) =>  myJoin(myJoin(myJoin(myJoin(r, r), r), r), s))
    execute(fileNumber, serial, "empty-consistency", (_) =>  myJoin(myJoin(myJoin(myJoin(s, r), r), r), r))

  }
}