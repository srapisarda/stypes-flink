package uk.ac.bbk.dcs.stypes.flink

import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment, Types}
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogTable, InMemoryExternalCatalog}
import org.apache.flink.table.descriptors._
import org.scalatest.FunSpec


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
      val expected = 0
      val rowCount = RelMetadataQuery.instance().getRowCount(s1.getRelNode)
      assert(rowCount == expected)

    }

//    it("should assert 0 as row count for relation S using calcite") {
//      val csvTable =
//    }

    it("should assert 0 as row count for relation S_EXT") {
      val catalogName = s"externalCatalog$fileNumber"
      val ec = getExternalCatalog(catalogName, 1,tableEnv)
      tableEnv.registerExternalCatalog(catalogName, ec)
      val s1 = tableEnv.scan("S_EXT")
      val expected = 0
      val rowCount = RelMetadataQuery.instance().getRowCount(s1.getRelNode)
      assert(rowCount == expected)

    }

  }

  def getExternalCatalog(catalogName: String, fileNumber: Int, tableEnv:BatchTableEnvironment): ExternalCatalog = {
    val cat = new InMemoryExternalCatalog(catalogName)
    // externalCatalog
    val externalCatalogTableS = getExternalCatalogTable() // new ExternalCatalogTable(true, false, true, false,)
    cat.createTable("S_EXT", externalCatalogTableS, ignoreIfExists = false)
    cat
  }

  private def getExternalCatalogTable(): ExternalCatalogTable = {
    // connector descriptor
    val connectorDescriptor = new FileSystem()
    connectorDescriptor.path(getFilePath(fileNumber, "S"))
    // Format
    val fd = new Csv()
    fd.field("X", Types.STRING)
    fd.field("Y", Types.STRING)
    fd.fieldDelimiter(",")
    // Statistic
    val statistics = new Statistics()
    statistics.rowCount(0)
    //Metadata
    val md = new Metadata()
    ExternalCatalogTable.builder(connectorDescriptor)
      .withFormat(fd)
      .withStatistics(statistics)
      .withMetadata(md)
      .asTableSource()
  }


}