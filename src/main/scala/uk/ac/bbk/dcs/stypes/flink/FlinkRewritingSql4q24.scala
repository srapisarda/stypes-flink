package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.flink.table.api.Table

object FlinkRewritingSql4q24 extends BaseFlinkTableRewriting {

  private def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit =
    executeTableRewriting(fileNumber, serial, "sql-q24", tableRewritingEvaluation)

  private def tableRewritingEvaluation(fileNumber: Int, jobName:String): Table = {

    val tableEnv = makeTableEnvironment(fileNumber, jobName)

    val a = tableEnv.sqlQuery("select X as a_x, X as a_y from A")
    val b = tableEnv.sqlQuery("select X as b_x, X as b_y from B")
    val s = tableEnv.sqlQuery("select X as s_x, Y as s_y from S")
    val r = tableEnv.sqlQuery("select X as r_x, Y as r_y from R")

    // p32(x7,x9) :- r(x7,x8), s(x8,x9).
    // p32(x9,x9) :- b(x9).
    val p32 =
    b.union(
      r.join(s).where("r_y=s_x").select("r_x as b_x, r_y as b_y"))
      .select("b_x as p32_x, b_y as p32_y")

    // p27(x12,x7) :- r(x7,x8), a(x8), r(x8,x11), r(x11,x12).
    val p27_1 = r
      .join(a).where("r_y=a_x").select("r_x as x7, a_x as x8")
      .join(r).where("x8=r_x").select("x7, r_y as x11")
      .join(r).where("x11=r_x").select("r_y as p27_x, x7 as p27_y")
    // p27(x12,x7) :- p32(x7,x9), r(x9,x10), r(x10,x11), r(x11,x12).
    val p27_2 = p32
      .join(r).where("p32_y=r_x").select("p32_x as x7, r_y as x10")
      .join(r).where("x10=r_x").select("x7, r_y as x11")
      .join(r).where("x11=r_x").select("r_y as P27_x, x7 as p27_y ")
    val p27 = p27_1.union(p27_2)

    // p5(x0,x3) :-  a(x0), r(x0,x3).
    val p5_1 = a.join(r).where("a_y=r_x").select("a_x as p5_x, r_y as p5_y")
    // p5(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3).
    val p5_2 = s
      .join(r).where("s_y=r_x").select("s_x as x0, r_y as x2")
      .join(r).where("x2=r_x").select("x0 as p5_x, r_y as p5_y")
    val p5 = p5_1.union(p5_2)

    // p3(x12,x8) :- a(x8), s(x8,x9), r(x9,x10),r(x10,x11), r(x11,x12).
    val p3_1 = a
      .join(s).where("a_y=s_x").select("a_x as x8, s_y as x9")
      .join(r).where("x9=r_x").select("x8, r_y as x10")
      .join(r).where("x10=r_x").select("x8, r_y as x11")
      .join(r).where("x11=r_x").select("r_y as p3_x, x8 as p3_y")
    // p3(x12,x8) :- a(x8), r(x8,x11), r(x11,x12).
    lazy val p3_2 = a
      .join(r).where("a_y=r_x").select("a_x as x8, r_y as x11")
      .join(r).where("x11=r_x").select("r_y as p3_x, x8 as p3_y")
    val p3 = p3_1 union p3_2

    // p1(x0,x12) :- p5(x0,x3), r(x3,x4),  r(x4,x5), r(x5,x6), a(x6), p3(x12,x6).
    val p1_1 = p5
      .join(r).where("p5_y=r_x").select("p5_x as x0, r_y as x4")
      .join(r).where("x4=r_x").select("x0, r_y as x5")
      .join(r).where("x5=r_x").select("x0, r_y as x6")
      .join(a).where("x6=a_x").select("x0, x6")
      .join(p3).where("x6=p3_y").select("x0 as x, p3_x as y")
    // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), b(x5),  p27(x12,x5).
    val p1_2 = p5
      .join(r).where("p5_y=r_x").select("p5_x as x0, r_y as x4")
      .join(r).where("x4=r_x").select("x0, r_y as x5")
      .join(b).where("x5=b_x").select("x0, x5")
      .join(p27).where("x5=p27_y").select("x0 as x, p27_x as y")
    // p1(x0,x12) :-  p5(x0,x3), r(x3,x4), r(x4,x5), r(x5,x6), s(x6,x7), p27(x12,x7).
    val p1_3 = p5
      .join(r).where("p5_y=r_x").select("p5_x as x0, r_y as x4")
      .join(r).where("x4=r_x").select("x0, r_y as x5")
      .join(r).where("x5=r_x").select("x0, r_y as x6")
      .join(s).where("x6=s_x").select("x0, s_y as x7")
      .join(p27).where("x7=p27_y").select("x0 as x, p27_x as y")
    val p1 = p1_1.union(p1_2).union(p1_3).distinct()
    p1
  }

  def main(args: Array[String]): Unit = {
    if ( args.length > 1 )
      FlinkRewritingSql4q24.run(args(0).toInt, args(1))
    else
      FlinkRewritingSql4q24.run(args(0).toInt)
  }

}
