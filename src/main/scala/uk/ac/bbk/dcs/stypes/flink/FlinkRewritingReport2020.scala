package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.api.scala.{DataSet, _}
// Scala object that defines the program in the main() method.
// Class Entry point: uk.ac.bbk.dcs.stypes.flink.FlinkRewritingReport2020
object FlinkRewritingReport2020 {
  // the main() execute the program
  def main(args: Array[String]): Unit = {
    // Set up the data execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // common path to hdfs file location
    val dataPath = "hdfs:////user/hduser/data/report2020"

    // create a DataSet[(String, String)] for the a file r.csv, s.csv and b.csv
    val r = env.readTextFile(s"$dataPath/r.csv").map(stringMapper)
    val s = env.readTextFile(s"$dataPath/s.csv").map(stringMapper)
    val b = env.readTextFile(s"$dataPath/b.csv").map(stringMapper)

    // operation for datalog: p1(x0,x3) :- r(x0,x1), r(x1,x2), s(x2,x3).
    val p1_0 = r.join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
                .join(s).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
    //  operation for datalog: p1(x0,x3) :- r(x0,x3), b(x3) .
    val p1_1 = r.join(b).where(1).equalTo(0).map(p => (p._1._1, p._2._1))
    r.map(p=> p._1)
    // p1 = P1_0 U P1_1
    val p1: DataSet[(String, String)] = p1_0.union(p1_1)
    // sink operation which white the p1 evaluation as file results.csv\item

    p1.writeAsCsv(s"$dataPath/result/" +
      s"${java.time.LocalDateTime.now().toString.replace(":","-")}.csv")

    // execute job
    env.execute("evaluating p1")
  }

  // String mapper function used to parse the CSV.
  // It gets in input a string return as output a tuple contains two elements
  def stringMapper: String => (String, String) = (p: String) => {
    val line = p.split(',')
    (line.head, line.last)
  }
}