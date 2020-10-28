package uk.ac.bbk.dcs.stypes.flink

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog._
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import _root_.scala.collection.JavaConversions._


/**
  * Created by salvo on 19/11/2018.
  */
class CalciteEmptyConsistencyTest extends FunSpec with BaseFlinkTest with Matchers with BeforeAndAfterAll {

  private val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
  private val bbTableEnv = TableEnvironment.create(bbSettings)

  private val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env, bbTableEnv.getConfig)
  tableEnv.getConfig        // access high-level configuration
    .getConfiguration   // set low-level key-value options
    .setString("table.optimizer.join-reorder-enabled", "true")

  private val catalogName = "S_CAT"
  private val databaseName = "S_EXT"
  private val tableNameS = "S"
  private val tableNameA = "A"
  private val tableNameR = "R"
  private val pathS = new ObjectPath(databaseName, tableNameS)
  private val pathA = new ObjectPath(databaseName, tableNameA)
  private val pathR = new ObjectPath(databaseName, tableNameR)


  private val catalog = getExternalCatalog(catalogName, 6)


  override def beforeAll(): Unit = {
    addStatisticsToCatalog()
    tableEnv.registerCatalog(catalogName, catalog)
    tableEnv.useCatalog(catalogName)
    tableEnv.useDatabase(databaseName)
    bbTableEnv.registerCatalog(catalogName, catalog)
    bbTableEnv.useCatalog(catalogName)
    bbTableEnv.useDatabase(databaseName)

    val calciteConfig: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(DataSetJoinRule.INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(DataSetUnionRule.INSTANCE,
        DataStreamRetractionRules.ACCMODE_INSTANCE)
      )
      .build()

    tableEnv.getConfig.setPlannerConfig(calciteConfig)
  }

  override def afterAll(): Unit = {

  }

  describe("Flink SQL  Empty test") {

    ignore("should assert 0 as row count for relation S") {
      val t: Table = tableEnv.scan(s"$tableNameS")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 0
      assert(rowCount == expected)
    }

    ignore("should assert 0 as row count for relation S using sql") {
      val t: Table = tableEnv.sqlQuery("select * from S")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 0
      assert(rowCount == expected)
    }

    it("should assert 0 as row count for relation R using sql") {
      val t: Table = tableEnv.sqlQuery("select * from R")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 4101642
      assert(rowCount == expected)
    }

    it("should assert 0 as row count for relation A using sql") {
      val t: Table = tableEnv.sqlQuery("select * from A")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 492
      assert(rowCount == expected)
    }

    it("should assert 0 as row count for query S inner join R using sql") {
      val t: Table = tableEnv.sqlQuery("select * from S inner join R on S.Y=R.X")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 0
      assert(rowCount == expected)
    }

    it("should assert 0 as row count for query S join R join A using sql") {
      val t: Table = tableEnv.sqlQuery("select * from S inner join R on S.Y=R.X inner join A on R.Y=A.X")
      val rowCount = tableEnv.toDataSet[Row](t).count()
      val expected = 0
      assert(rowCount == expected)
    }

    ignore("should assert 0 as row count for query R join R join A join S using sql") {
      //      addStatisticsToCatalog()
      val t: Table = tableEnv.sqlQuery("select r1.X, S.Y from R as r1 " +
        "inner join R as r2 on r1.Y=r2.X inner join A on r2.Y=A.X " +
        "inner join S on S.X=A.X")

      //val explanation = tableEnv.explain(false)
      //println(explanation)

      val actual = tableEnv.toDataSet[Row](t).count()
      val expected = 0
      assert(0 == expected)
    }

  }


  def getExternalCatalog(catalogName: String, fileNumber: Int): Catalog = {
    val catalog = new GenericInMemoryCatalog(catalogName, databaseName)
    // external Catalog table
    val externalCatalogTableS =  getExternalCatalogTable(tableNameS, fileNumber)
    val externalCatalogTableA = getExternalCatalogTable(tableNameA, fileNumber)
    val externalCatalogTableR = getExternalCatalogTable(tableNameR, fileNumber)
    // add external Catalog table
    catalog.createTable(pathS, externalCatalogTableS, false)
    catalog.createTable(pathA, externalCatalogTableA, false)
    catalog.createTable(pathR, externalCatalogTableR, false)

    catalog
  }

  private def getTableSchema(filename: String) = {
    val builder = TableSchema.builder()
    builder.field("X", DataTypes.STRING)
    if (filename != tableNameA)
      builder.field("Y", DataTypes.STRING)

    builder.build()
  }

  private def getFileFormat(fileName: String) = {
    val csv = new OldCsv()
    csv.field("X", Types.STRING)
    if (fileName != tableNameA)
      csv.field("Y", Types.STRING())

    csv
  }

  private def getExternalCatalogTable(fileName: String, fileNumber: Int): CatalogBaseTable = {
    val connectorDescriptor = new FileSystem()
    val resourcePath = this.getClass.getResource(getFilePathAsResource(fileNumber, fileName)).getPath
    connectorDescriptor.path(resourcePath)

    new CatalogTableBuilder(connectorDescriptor, getTableSchema(fileName))
      .withFormat(getFileFormat(fileName))
      .build()

  }

  private def addStatisticsToCatalog() = {
    val tableStatisticsS = new CatalogTableStatistics(0, 1, 0, 0)
    catalog.alterTableStatistics(pathS, tableStatisticsS, false)
    val tableStatisticsR = new CatalogTableStatistics(4101642, 1, 46940747, 46940747)
    catalog.alterTableStatistics(pathR, tableStatisticsR, false)
    val tableStatisticsA = new CatalogTableStatistics(492, 1, 2807, 2807)
    catalog.alterTableStatistics(pathA, tableStatisticsA, false)
  }

  private def getBatchTableProperties = {
    val map = Map("is_streaming" -> "false")
    mapAsJavaMap(map)
  }


}
