package uk.ac.bbk.dcs.stypes.flink

/*
 * #%L
 * STypeS
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.scalatest.FunSpec

/**
 * Created by salvo on 01/01/2018.
 *
 * Flink test based on the rewriting for query q27.cq  using  lines.dlp
 *
 * p32(x7,x9) :- r(x7,x8), s(x8,x9).
 * p32(x9,x9) :- b(x9).
 * p27(x12,x7) :- a(x8), r(x7,x8), r(x11,x12), r(x8,x11).
 * p27(x12,x7) :- r(x9,x10), p32(x7,x9), r(x11,x12), r(x10,x11).
 * p5(x0,x3) :- r(x0,x3), a(x0).
 * p5(x0,x3) :- r(x1,x2), r(x2,x3), s(x0,x1).
 * p3(x12,x8) :- r(x10,x11), r(x11,x12), a(x8), s(x8,x9), r(x9,x10).
 * p3(x12,x8) :- a(x8), r(x11,x12), r(x8,x11).
 * p1(x0,x12) :- p3(x12,x6), r(x3,x4), r(x5,x6), r(x4,x5), p5(x0,x3), a(x6).
 * p1(x0,x12) :- p5(x0,x3), r(x4,x5), b(x5), r(x3,x4), p27(x12,x5).
 * p1(x0,x12) :- s(x6,x7), p5(x0,x3), r(x4,x5), r(x5,x6), r(x3,x4), p27(x12,x7).
 *
 */
class FlinkQ27TableTest extends FunSpec with BaseFlinkTableTest {

  describe("Flink Talbe q27 ") {
    it("should execute p27 using file 6") {
      cleanSink()
      makeCatalog(3)

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
      val p1= p1_1.union(p1_2).union(p1_3)


      println(tableEnv.explain(p1))

      p1.insertInto(tableNameSink1)

      //val count = tableEnv sqlQuery( s"select count(X) as X from $tableNameSink1" )

      //count.insertInto(tableNameSinkCount)

      tableEnv.execute("Q27 sql")
    }
  }

}
