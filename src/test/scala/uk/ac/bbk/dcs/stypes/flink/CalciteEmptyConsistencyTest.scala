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
class CalciteEmptyConsistencyTest extends FunSpec with BaseFlinkTest {
  val calciteConfigBuilder = new CalciteConfigBuilder()
  val ruleSets: RuleSet = RuleSets.ofList(
    LoptOptimizeJoinRule.INSTANCE)
  calciteConfigBuilder.addLogicalOptRuleSet(ruleSets)
  val tableConfig: TableConfig = new TableConfig()
  tableConfig.setCalciteConfig(calciteConfigBuilder.build())
  private val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env, tableConfig)

  private val fileNumber = 1
  tableEnv.registerTableSource("S", getDataSourceS(fileNumber))


  describe("Flink SQL  Empty test") {

    it("should assert 0 as row count for relation S") {
      val s1 = tableEnv.scan("S")
      val actual = 0
      val rowCount = RelMetadataQuery.instance().getRowCount(s1.getRelNode)
      assert(rowCount == actual)

    }

//    it("should assert 0 as row count for relation S using calcite") {
//      val csvTable =
//    }

  }


}