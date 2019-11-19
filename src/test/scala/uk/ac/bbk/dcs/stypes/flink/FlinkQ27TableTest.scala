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
 * Flink test based on the rewriting for query q15.cq  using  lines.dlp
 *
 * p1(x0,x7) :- r(x3,x4), p12(x7,x4), p3(x0,x3).
 * p3(x0,x3) :- r(x0,x3), a(x0).
 * p3(x0,x3) :- r(x1,x2), r(x2,x3), s(x0,x1).
 * p12(x7,x4) :- r(x5,x6), r(x4,x5), s(x6,x7).
 * p12(x7,x4) :- r(x4,x7), b(x7).
 *
 */
class FlinkQ27TableTest extends FunSpec with BaseFlinkTableTest {

  describe("Flink Talbe q27 ") {
    it("should execute p27 using file 6") {
      cleanSink()
      makeCatalog(3)

      // p12(x7,x4) :- r(x4,x7), b(x7).
      val p12_1 =
        "select b1.X as X, r1.X as Y " +
        "from R as r1 " +
          "inner join B as b1 on r1.Y=b1.X"

      // p12(x7,x4) :- r(x4,x7), b(x7).
      val p12_2 =
        "select r1.Y as X, r1.X as Y " +
        "from R as r1 " +
          "inner join B as b1 on r1.Y=b1.X"

      val p12Sql=s"select X as p12_X, Y as p12_Y \n" +
        s"from (\n ($p12_1) \n" +
        s"union \n" +
        s"($p12_2) \n)"

      println(s"p12 -> :$p12Sql")
      val p12 = tableEnv.sqlQuery(p12Sql)

      // p3(x0,x3) :- r(x1,x2), r(x2,x3), s(x0,x1).
      val p3_1 =
        "select s1.X as X, r2.Y as Y " +
        "from R as r1 " +
          "inner join R as r2 on r1.Y=r2.X " +
          "inner join S as s1 on r1.X=s1.Y "

      // p3(x0,x3) :- r(x0,x3), a(x0).
      val p3_2 =
        "select r1.X as X, r1.Y " +
          "from R as r1 " +
          "inner join A as a1 on a1.X=r1.X"

      val p3Sql = s"select X as p3_X, Y as p3_Y \n" +
        s"from( \n" +
        s"($p3_1) \n" +
        s"union \n" +
        s"($p3_2))"

      println(s"p3 -> $p3Sql")
      val p3: Table = tableEnv.sqlQuery(p3Sql)


      val r = tableEnv.sqlQuery("select X as r_X, Y as r_Y from R")


      // p1(x0,x7) :- r(x3,x4), p12(x7,x4), p3(x0,x3).
      val p1 = r.join(p12)
        .where( "r_Y=p12_Y")
        .select("*")
        .join(p3).
        where("r_X=p3_Y")
        .select("p3_X as X, p12_X as Y")
          .distinct()

      println( tableEnv.explain(p1) )

      p1.insertInto(tableNameSink1)


      //val count = tableEnv sqlQuery( s"select count(X) as X from $tableNameSink1" )

      //count.insertInto(tableNameSinkCount)

      tableEnv.execute("Q27 sql")
    }
  }

}
