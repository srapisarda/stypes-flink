package uk.ac.bbk.dcs.stypes.flink

import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment}
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.types.Row
import org.scalatest.FunSpec

/**
  * Created by salvo on 19/11/2018.
  */
class EmptyConsistencySQLTest extends FunSpec with BaseFlinkTest {
  val calciteConfigBuilder = new CalciteConfigBuilder()
  val ruleSets: RuleSet = RuleSets.ofList(JoinCommuteRule.INSTANCE, JoinAssociateRule.INSTANCE, LoptOptimizeJoinRule.INSTANCE)
  calciteConfigBuilder.addLogicalOptRuleSet(ruleSets)
  val tableConfig: TableConfig = new TableConfig()
  tableConfig.setCalciteConfig(calciteConfigBuilder.build())
  private val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env, tableConfig)


  private val fileNumber = 1
  tableEnv.registerTableSource("R", getDataSourceR(fileNumber))
  tableEnv.registerTableSource("S", getDataSourceS(fileNumber))
  private lazy val r = tableEnv.scan("R")
  private lazy val s = tableEnv.scan("S")
  //  private val rTableSet = tableEnv.toDataSet[Row](r)
  //  private val sTableSet = tableEnv.toDataSet[Row](s)


  describe("Flink SQL  Empty test") {

    it("should read and execute the EmptyConsistencySQL for query q(x1,x4) <- R(x1,x2), R(x2,x3), S(x3,x4)") {

      val table = r.as("x1, x2").join(r.as("x3, x4")).where("x2 === x3").select("x1, x4")
        .join(s.as("x5, x6")).where("x4 === x5 ").select("x1, x6")

      val result = executeAsTable(1, "test", "empty-consistency", _ => {
        r.as("x1, x2").join(r.as("x3, x4")).where("x2 === x3").select("x1, x4")
          .join(s.as("x5, x6")).where("x4 === x5 ").select("x1, x6")
      })

      assert(result.count() == 0)

    }

    it("should read and execute the EmptyConsistencySQL  query for the sets {q(x1,x4) <- S(x1,x2), R(x2,x3), R(x3,x4)") {
      val result = executeAsTable(1, "test", "empty-consistency", _ => {
        s.as("x1, x2").join(r.as("x3, x4")).where("x2 === x3").select("x1, x4")
          .join(r.as("x5, x6")).where("x4 === x5 ").select("x1, x6")
      })

      assert(result.count() == 0)

    }

  }


  private def executeAsTable(fileNumber: Int, serial: String, qName: String, f: Int => Table): DataSet[Row] = {
    val startTime = System.nanoTime()
    distinctTableSink(f.apply(fileNumber), fileNumber, serial, startTime, qName)
  }

  private def distinctTableSink(p1: Table, fileNumber: Int, serial: String, startTime: Long, qName: String): DataSet[Row] = {
    val p1_distinct = p1.distinct()

    val explanation: String = tableEnv.explain(p1_distinct)

    println(explanation)

    p1.groupBy("x1").select("x1.count")

    val count: Table =
      p1_distinct
        .groupBy("x1")
        .select("x1 as cnt")


    val result: DataSet[Row] = tableEnv.toDataSet[Row](count)

    result

  }

}