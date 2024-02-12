package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.api.scala._

object FlinkRewritingThesis {
  // the main() execute the program
  def main(args: Array[String]): Unit = {
    // Set up the data execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // common path to AWS s3 file location
    val dataPath = "s3://stypes-datasets/data"

    // create a DataSet[(String, String)] for the a file r.csv, s.csv and b.csv
    val r = env.readTextFile(s"$dataPath/csv/3.ttl-R.csv").map(stringMapper2)
    val s = env.readTextFile(s"$dataPath/csv/3.ttl-S.csv").map(stringMapper2)
    val t = env.readTextFile(s"$dataPath/csv/3.ttl-T.csv").map(stringMapper2)

    // operation for datalog: P S1
    //P(x) <- T (z, v), R(y, z), S(x, y),
    val p1_0 = t.join(r).where(0).equalTo(1).map(p => Tuple1(p._2._1))
                .join(s).where(0).equalTo(1).map(p => Tuple1(p._2._1))
    //  operation for datalog: P S1
    //P (x) <- S(x, y), S(y, v).
    val p1_1 = s.join(s).where(1).equalTo(0).map(p => Tuple1(p._1._1))
    // p1 = P1_0 U P1_1
    val p1 = p1_0.union(p1_1)
    // sink operation which white the p1 evaluation as file results.csv\item
    p1.writeAsCsv(s"$dataPath/completed-jobs/" +
     s"thesis-ex1-${java.time.LocalDateTime.now().toString.replace(":","-")}.csv")

    // execute job
    env.execute("evaluating p1")
  }

  // String mapper function used to parse the CSV.
  // It gets in input a string return as output a tuple contains two elements
  def stringMapper2: String => (String, String) = (p: String) => {
    val line = p.split(',')
    (line.head, line.last)
  }

  def stringMapper1: (String) => (String) = (p: String) => {
    val line = p.split(',')
    line.head
  }
}