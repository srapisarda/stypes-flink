package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.api.scala._

// uk.ac.bbk.dcs.stypes.flink.FlinkRewritingQ30
// Scala object that defines the program in the main() method.
// Class Entry point
object FlinkRewritingQ30{
  // the main() execute the program
  def main(args: Array[String]): Unit = {
    // Set up the data execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //**EDB-MAP**
    val s = env.readTextFile("hdfs:////user/hduser/data/report2020/s.csv").map(stringMapper2)
    val r = env.readTextFile("hdfs:////user/hduser/data/report2020/r.csv").map(stringMapper2)
    val a = env.readTextFile("hdfs:////user/hduser/data/report2020/a.csv").map(stringMapper2)
    val b = env.readTextFile("hdfs:////user/hduser/data/report2020/b.csv").map(stringMapper2)

    //**MAIN**

    // p45[2](x11,x8) :- r[2](x9,x10), s[2](x8,x9), r[2](x10,x11)
    lazy val p45_0 = r.join(s).where(0).equalTo(1).map(p => (p._1._2, p._2._1))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))

    // p45[2](x11,x8) :- a[1](x8), r[2](x8,x11)
    lazy val p45_1 = a.join(r).where(0).equalTo(0).map(p => (p._2._1,p._2._2))

    lazy val p45 = p45_0 union p45_1

    // p5[2](x0,x3) :- r[2](x1,x2), r[2](x2,x3), s[2](x0,x1)
    lazy val p5_0 = r.join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(s).where(0).equalTo(1).map(p => (p._1._2, p._2._1))

    // p5[2](x0,x3) :- r[2](x0,x3), a[1](x0)
    lazy val p5_1 = r.join(a).where(0).equalTo(0).map(p => (p._1._2, p._2._1))

    lazy val p5 = p5_0 union p5_1

    // p1[2](x0,x15) :- p5[2](x0,x3), p14[2](x9,x4), r[2](x3,x4), r[2](x14,x15), r[2](x12,x13), r[2](x13,x14), r[2](x10,x11), b[1](x9), r[2](x9,x10), r[2](x11,x12)
    lazy val p1_0 = p5.join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(p14).where(1).equalTo(1).map(p => (p._1._1, p._2._1))
      .join(b).where(0).equalTo(0).map(p => (p._1._1, p._2._1))
      .join(r).where(0).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))

    // p1[2](x0,x15) :- r[2](x7,x8), p5[2](x0,x3), p14[2](x7,x4), r[2](x3,x4), r[2](x14,x15), r[2](x12,x13), r[2](x13,x14), p45[2](x11,x8), r[2](x11,x12)
    lazy val p1_1 = r.join(p14).where(0).equalTo(0).map(p => (p._1._2, p._2._2))
      .join(r).where(1).equalTo(1).map(p => (p._1._2, p._2._1))
      .join(p5).where(0).equalTo(1).map(p => (p._1._2, p._2._1))
      .join(p45).where(1).equalTo(1).map(p => (p._1._1, p._2._1))
      .join(r).where(0).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))

    // p1[2](x0,x15) :- p5[2](x0,x3), r[2](x4,x5), a[1](x6), r[2](x5,x6), r[2](x3,x4), r[2](x14,x15), r[2](x12,x13), r[2](x13,x14), p45[2](x11,x6), r[2](x11,x12)
    lazy val p1_2 = p5.join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(a).where(1).equalTo(0).map(p => (p._1._1,p._1._2))
      .join(p45).where(0).equalTo(1).map(p => (p._1._1, p._2._1))
      .join(r).where(0).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(1).equalTo(0).map(p => (p._1._1, p._2._2))

    lazy val p1 = p1_0 union p1_1 union p1_2

    // p14[2](x7,x4) :- b[1](x7), r[2](x4,x7)
    lazy val p14_0 = b.join(r).where(0).equalTo(1).map(p => (p._2._1,p._2._2))

    // p14[2](x7,x4) :- r[2](x5,x6), s[2](x6,x7), r[2](x4,x5)
    lazy val p14_1 = r.join(s).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
      .join(r).where(0).equalTo(1).map(p => (p._1._2, p._2._1))

    lazy val p14 = p14_0 union p14_1

    //**SINK**
    p1.writeAsCsv("hdfs:////user/hduser/data/report2020/result/2020-10-25T16-30-40.909.csv")

    // execute job
    env.execute("job rewriting q30")
  }

  //**MAPPER-FUNC
  // stringMapper2
  // String mapper function used to parse the CSV.
  // It gets in input a string return as output a tuple contains two elements
  def stringMapper2: String => (String, String) = (p: String) => {
    val line = p.split(',')
    (line.head, line.last)
  }
}



