package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.{BatchTableEnvironment, _}
import org.apache.flink.table.catalog._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row
import org.scalatest.{FunSpec, Matchers}

import _root_.scala.collection.JavaConversions._


/**
  * Created by salvo on 19/11/2018.
  */
class CalciteEmptyConsistencyTest extends FunSpec with BaseFlinkTest with Matchers {
//  val calciteConfigBuilder = new CalciteConfigBuilder()
//
//  val ruleSets: RuleSet = RuleSets.ofList(
//    LoptOptimizeJoinRule.INSTANCE)
//  calciteConfigBuilder.addLogicalOptRuleSet(ruleSets)
//  val tableConfig: TableConfig = new TableConfig()
//  tableConfig.setCalciteConfig(calciteConfigBuilder.build())
  private val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
  private val fileNumber = 1
  private val model:String = "src/test/resources/benchmark/Lines/data/model"

  val catalogName = "S_CAT"
  val databaseName = "S_EXT"
  val tableName = "S"

  describe("Flink SQL  Empty test") {

    it("should assert 0 as row count for relation S") {
      val catalog = getExternalCatalog(catalogName,1)
      tableEnv.registerCatalog(catalogName, catalog)
      tableEnv.useCatalog(catalogName)
      tableEnv.useDatabase(databaseName)
      val t: Table = tableEnv.scan(s"$tableName")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 0
//      val rowCount = 1 //RelMetadataQuery.instance().getRowCount(s1.)
      assert(rowCount == expected)

    }

  }

  def getExternalCatalog(catalogName: String, fileNumber: Int): Catalog = {
    val catalog = new GenericInMemoryCatalog(catalogName, databaseName)
      // external Catalog table
    val externalCatalogTableS = getExternalCatalogTable(tableName)
    // add external Catalog table
    catalog.createTable( new ObjectPath( databaseName, tableName ), externalCatalogTableS, false)
    catalog
  }

  private def getTableSchema = {
    TableSchema.builder()
      .field("X", DataTypes.STRING)
      .field("Y", DataTypes.STRING)
      .build()
  }

  private def getFileFormat =
    new OldCsv().field("X", Types.STRING).field("Y", Types.STRING())

  private def getExternalCatalogTable(fileName: String): CatalogBaseTable = {
    val connectorDescriptor = new FileSystem()
    val resourcePath = this.getClass.getResource(getFilePathAsResource(fileNumber, tableName)).getPath
    connectorDescriptor.path(resourcePath)

    new CatalogTableBuilder(connectorDescriptor, getTableSchema)
      .withFormat(getFileFormat)
      .build()

  }

  private def getBatchTableProperties = {
    val map = Map("is_streaming"->"false")
    mapAsJavaMap(map)
  }


}
