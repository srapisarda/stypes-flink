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

import org.apache.flink.table.api.Table
import org.scalatest.FunSpec

/**
 * Created by salvo on 01/01/2018.
 *
 * Flink test based on the rewriting for query q24.cq  using  lines.dlp
 *
 */
class FlinkQ24TableTest extends FunSpec with BaseFlinkTableTest {

  describe("Flink q24 table test") {

    ignore("should read and execute the 'q24.cq' query rewrote for 1.ffl file set {A, B, R, S}") {
      execute(1, 2832 )
    }

    ignore("should read and execute the 'q24.cq' query rewrote for 2.ffl file set {A, B, R, S}") {
      execute(2, 248)
    }

    it("should read and execute the 'q24.cq' query rewrote for 3.ffl file set {A, B, R, S}") {
      execute(3, 2125)
    }

    it("should read and execute the 'q24.cq' query rewrote for 4.ffl file set {A, B, R, S}") {
      execute(4, 62572)
    }

    ignore("should read and execute the 'q24.cq' query rewrote for 5.ffl file set {A, B, R, S}") {
      execute(5, 119951)
    }

    ignore("should read and execute the 'q24.cq' query rewrote for 6.ffl file set {A, B, R, S}") {
      execute(6, 105467)
    }


    def execute(fileNumber:Int, expected:Int) = {
      cleanSink()
      makeCatalog(fileNumber)

      val a = tableEnv.sqlQuery("select X as a_x, X as a_y from A")
      val b = tableEnv.sqlQuery("select X as b_x, X as b_y from B")
      val s = tableEnv.sqlQuery("select X as s_x, Y as s_y from S")
      val r = tableEnv.sqlQuery("select X as r_x, Y as r_y from R")

      // p11(x9,x9) :- b(x9).
      // p11(x7,x9) :- r(x7,x8), s(x8,x9).
      val p11 = b.select("b_x as x, b_y as y")
        .union(r.join(s).where("r_y=s_x").select("r_x as x, s_y as y"))
        .select("x as p11_x, y as p11_y")

      // p5(x0,x2) :- s(x0,x1), r(x1,x2).
      // p5(x0,x0) :- a(x0).
      val p5 = s.join(r).where("s_y=r_x").select("s_x as x, r_y as y")
        .union(a.select("a_x as x, a_y as y"))
        .select("x as p5_x, y as p5_y")

      // p17(x9,x5) :- b(x5), p11(x5,x9).
      // p17(x9,x5) :- r(x5,x8), a(x8), s(x8,x9).
      // p17(x9,x5) :-  r(x5,x6), s(x6,x7), p11(x7,x9).
      val p17_1 = b.join(p11).where("b_y=p11_x").select("b_x as p17_x, p11_y as p17_y")
      val p17_2 = (r.join(a).where("r_y=a_x").select("r_x, a_y"))
        .join(s).where("a_y=s_x").select("s_y as  p17_x, r_x as  p17_y")
      val p17_3 = (r.join(s).where("r_y=s_x").select("r_x, s_y"))
        .join(p11).where("s_y=p11_x").select("p11_x as p17_x, r_x as p17_y")
      val p17 = p17_1.union(p17_2).union(p17_3)

      // p1(x0,x9) :-  p5(x0,x2), r(x2,x3), r(x3,x4), r(x4,x5), p17(x9,x5).
      val p1 = p5.join(r).where("p5_y=r_x").select("p5_x, r_y as y")
        .join(r).where("y=r_x").select("p5_x, r_y as y")
        .join(r).where("y=r_x").select("p5_x, r_y as y")
        .join(p17).where("y=p17_x").select(" p5_x as x, p17_y as y").distinct()


      println(tableEnv.explain(p1))
      p1.insertInto(tableNameSink1)

      var count = p1.groupBy("x").select("x.count as count")
      count.insertInto(tableNameSinkCount)

      tableEnv.execute("Q24 sql")

//      val p1_count = p1.count

    }
  }

}
