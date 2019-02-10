package uk.ac.bbk.dcs.stypes.flink

import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.optimizer.{DataStatistics, Optimizer}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment}
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.types.Row
import org.scalatest.FunSpec

import scala.util.Try


/**
  * Created by salvo on 19/11/2018.
  */
class EmptyConsistencySQLTest extends FunSpec with BaseFlinkTest {
  val calciteConfigBuilder = new CalciteConfigBuilder()
  val ruleSets: RuleSet = RuleSets.ofList(
    LoptOptimizeJoinRule.INSTANCE)
  calciteConfigBuilder.addLogicalOptRuleSet(ruleSets)
  val tableConfig: TableConfig = new TableConfig()
  tableConfig.setCalciteConfig(calciteConfigBuilder.build())
  private val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env, tableConfig)


  private val fileNumber = 1
  tableEnv.registerTableSource("R", getDataSourceR(fileNumber))
  tableEnv.registerTableSource("S", getDataSourceS(fileNumber))


  private val r = tableEnv.scan("R")
  private val s = tableEnv.scan("S")

  var relMetadataQuery = RelMetadataQuery.instance()

  val rowCount = RelMetadataQuery.instance().getRowCount(s.getRelNode)
  val sRowCount = s.getRelNode.estimateRowCount(RelMetadataQuery.instance())
  //  (new JaninoRelMetadataProvider(DefaultRelMetadataProvider.INSTANCE), false))


  private val rTableSet = tableEnv.toDataSet[Row](r)
  private val sTableSet = tableEnv.toDataSet[Row](s)

  private val qc = tableEnv.queryConfig


  describe("Flink SQL  Empty test") {

    it("should read and execute the EmptyConsistencySQL for query q(x1,x4) <- R(x1,x2), R(x2,x3), S(x3,x4)") {

      val result = executeAsTable(1, "test", "empty-consistency", _ => {
        r.as("x1, x2").join(r.as("x3, x4")).where("x2 === x3").select("x1, x4")
          .join(s.as("x5, x6")).where("x4 === x5").select("x1, x6")
      })

      assert(result.count() == 0)

    }

    it("should read and execute the EmptyConsistencySQL  query for the sets {q(x1,x4) <- S(x1,x2), R(x2,x3), R(x3,x4)") {
      val result = executeAsTable(1, "test", "empty-consistency", _ => {
        s.as("x1, x2").join(r.as("x3, x4")).where("x2 === x3").select("x1, x4")
          .join(r.as("x5, x6")).where("x4 === x5").select("x1, x6")
      })

      assert(result.count() == 0)

    }

    it("should assert 0 as row count for relation S") {
      val s1 = tableEnv.scan("S")
      val actual = 0
      val rowCount = RelMetadataQuery.instance().getRowCount(s1.getRelNode)
      assert(rowCount == actual)
    }


    it("should create a plan from an iteration") {
      val test = Try {
        val data = new DataStatistics()
        val optimiser = new Optimizer(data, new Configuration())

        optimiser.setDefaultParallelism(env.getParallelism)
        val input: DataSet[Long] = env.generateSequence(1, 100)

        val iteration = input.iterateWithTermination(100) {
          previous =>
            val next = previous.map { x => x + 1 }
            val term = next.filter {
              _ < 3
            }
            (next, term)
        }

        iteration.output(new DiscardingOutputFormat())

        val plan = env.createProgramPlan()
        val op = optimiser.compile(plan)

        println(op)
      }
      if (test.isFailure) fail()

    }
  }


  private def executeAsTable(fileNumber: Int, serial: String, qName: String, f: Unit => Table) = {
    val p1 = f.apply()
    println(tableEnv.explain(p1))
    val count = p1.groupBy("x1").select("x1 as cnt")
    val result = tableEnv.toDataSet[Row](count)

    result
  }


}