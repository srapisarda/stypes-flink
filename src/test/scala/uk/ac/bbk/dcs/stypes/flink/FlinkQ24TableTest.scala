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
 * Flink test based on the rewriting for query q24.cq  using  lines.dlp
 *
 */
class FlinkQ24TableTest extends FunSpec with BaseFlinkTableTest {

  describe("Flink q24 table test") {
    cleanSink()
    val a = tableEnv.sqlQuery("select X as a_x, X as a_y from A")
    val b = tableEnv.sqlQuery("select X as b_x, X as b_y from B")
    val s = tableEnv.sqlQuery("select X as s_x, Y as s_y from S")
    val r = tableEnv.sqlQuery("select X as r_x, Y as r_y from R")


    // p11(x9,x9) :- b(x9).
    // p11(x7,x9) :- r(x7,x8), s(x8,x9).
    val p11 = b
      .union(r.join(s).where("r_y=s_x").select("r_x as b_x, s_y as b_y"))
      .select("b_x as p11_x, b_y as p11_y")

    // p5(x0,x2) :- s(x0,x1), r(x1,x2).
    // p5(x0,x0) :- a(x0).
    val p5 = s.join(r).where("s_y=r_x").select("s_x as a_x, r_y as a_y")
      .union(a)
      .select("a_x as p5_x, a_y as p5_y")


    // p17(x9,x5) :- b(x5), p11(x5,x9).
    // p17(x9,x5) :- r(x5,x8), a(x8), s(x8,x9).
    // p17(x9,x5) :-  r(x5,x6), s(x6,x7), p11(x7,x9).
    val p17 = b.join(p11).where("b_y=p11_x").select("b_x as p17_x, b_y as p17_y")
      .union(
        r.join(a).where("r_y=a_x").select("r_x, a_y")
          .join(s).where("a_y=s_x").select("s_y as  p17_x, r_x as  p17_y"))
      .union(
        r.join(s).where("r_y=s_x").select("r_x, s_y")
          .join(p11).where("s_y=p11_x").select("p11_x as p17_x, r_x as p17_y"))

        // p1(x0,x9) :-  p5(x0,x2), r(x2,x3), r(x3,x4), r(x4,x5), p17(x9,x5).
        val p1 = p5.join(r).where("p5_y=r_x").select("p5_x, r_y as r1_y")
          .join(r).where("r_x=r1_y").select("p5_x, r_y as r2_y")
          .join(r).where("r_x=r2_y").select("p5_x, r_y as r3_y")
          .join(p17).where("p17_y=r3_y").select("p17_y as Y, p5_x as X")


    println(tableEnv.explain(p17))
    p1.insertInto(tableNameSink1)

    tableEnv.execute("Q24 sql")

  }

}
