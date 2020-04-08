package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.calcite.tools.RuleSets
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.catalog.{ConnectorCatalogTable, ObjectPath}
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import org.junit.Assert.assertNotNull
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}

class FlinkCatalogStatisticTest extends FunSpec with BaseFlinkTest with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  private val catalogName = "S_CAT"
  private val databaseName = "default_database"
  private val tableNameS = "S"
  private val tableNameA = "A"
  private val tableNameR = "R"
  private val tableNameSink1 = s"sink_1"
  private val tableNameSink2 = s"sink_2"
  private val pathS = new ObjectPath(databaseName, tableNameS)
  private val pathA = new ObjectPath(databaseName, tableNameA)
  private val pathR = new ObjectPath(databaseName, tableNameR)
  private val pathSink1 = new ObjectPath(databaseName, tableNameSink1)
  private val pathSink2 = new ObjectPath(databaseName, tableNameSink2)

  private val fileNumber = 6

  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
  private val tableEnv: TableEnvironment = TableEnvironment.create(settings)
  tableEnv.getConfig // access high-level configuration
    .getConfiguration // set low-level key-value options
    .setString("table.optimizer.join-reorder-enabled", "true")

  private val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).orElse(null)

  override def beforeAll(): Unit = {
    cleanSink()

    tableEnv.registerCatalog(catalogName, catalog)
    tableEnv.useCatalog(catalogName)
    tableEnv.useDatabase(databaseName)

    assertNotNull(catalog)
    catalog.createTable(pathS,
      ConnectorCatalogTable.source[Row](getExternalCatalogSourceTable(tableNameS, fileNumber), true),
      false)
    catalog.createTable(pathA,
      ConnectorCatalogTable.source[Row](getExternalCatalogSourceTable(tableNameA, fileNumber), true),
      false)
    catalog.createTable(pathR,
      ConnectorCatalogTable.source[Row](getExternalCatalogSourceTable(tableNameR, fileNumber), true),
      false)

    catalog.createTable(pathSink1,
      ConnectorCatalogTable.sink[Row](getExternalCatalogSinkTable(tableNameSink1, fileNumber), true),
      false
    )

    catalog.createTable(pathSink2,
      ConnectorCatalogTable.sink[Row](getExternalCatalogSinkTable(tableNameSink2, fileNumber), true),
      false
    )

    catalog.alterTableStatistics(pathS,
      new CatalogTableStatistics(0, 1, 0L, 0L),
      true)

    catalog.alterTableStatistics(pathR,
      new CatalogTableStatistics(4101642, 1, 46940747L, 46940747 * 2L),
      true)

    catalog.alterTableStatistics(pathA,
      new CatalogTableStatistics(492, 1, 2807L, 2807 * 2L),
      true)

    catalog.open()
    // change calcite configuration
    val calciteConfig: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(DataSetJoinRule.INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(DataSetUnionRule.INSTANCE,
        DataStreamRetractionRules.ACCMODE_INSTANCE)
      )
      .build()

    tableEnv.getConfig.setPlannerConfig(calciteConfig)

  }

  describe("Create flink catalog whit statistic") {

    it("should execute the sql query ") {
      val table = tableEnv.sqlQuery("select X, X from A")
      table.insertInto(tableNameSink1)
      tableEnv.execute("mytest1")
    }


    it("should create statistics and apply them in order to create a plan") {
      val table = tableEnv.sqlQuery("select r1.X, A.X from R as r1 " +
        "inner join R as r2  on r1.Y=r2.X " +
        "inner join S on r2.Y = S.X " +
        "inner join R as r3  on S.Y=r3.X " +
        "inner join A on r3.X=A.X")
      val plan = tableEnv.explain(table)
      println(plan)

      // table.insertInto(tableNameSink2)
      //tableEnv.execute("mytest2")
    }

    ignore("should create statistics and apply them in order to create a plan 3") {
      val tableNameSink = createTableSink(UUID.randomUUID().toString.replaceAll("-", "_"))

      val table = tableEnv.sqlQuery("select r1.X, r2.X from R as r1 " +
        "inner join R as r2 on r1.Y=r2.X ")
      val plan = tableEnv.explain(table)
      println(plan)

      table.insertInto(tableNameSink)
      tableEnv.execute("mytest2")
    }

  }

  private def newUUID = UUID.randomUUID().toString.replaceAll("-", "_")

  private def cleanDir(path: String) = {
    val dir = FileUtils.getFile(path)
    if (dir.exists() && dir.isDirectory)
      FileUtils.cleanDirectory(dir)
  }

  private def cleanSink() = {
    val resourcePath = this.getClass.getResource(getFilePathFolderAsResource).getPath
    cleanDir(s"$resourcePath/sink")
  }

  private def getResultSinkPath(fileName: String, fileNumber: Int) = {
    val resourcePath = this.getClass.getResource(getFilePathFolderAsResource).getPath
    s"$resourcePath/sink/$fileName-$fileNumber-sink"
  }

  private def createTableSink(uuid: String): String = {
    val tableNameSink = s"sink_$uuid"

    println(s"table Name Sink: $tableNameSink")
    val pathSink = new ObjectPath(databaseName, tableNameSink)
    catalog.open()
    catalog.createTable(pathSink,
      ConnectorCatalogTable.sink(getExternalCatalogSinkTable(tableNameSink, fileNumber), true),
      true)
    catalog.close()
    tableNameSink
  }

  private def getExternalCatalogSinkTable(fileName: String, fileNumber: Int): TableSink[Row] = {
    val csvTableSink = new CsvTableSink(getResultSinkPath(fileName, fileNumber))
    val fieldNames: Array[String] = Array("X", "Y")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING)
    csvTableSink.configure(fieldNames, fieldTypes)
  }

  private def getExternalCatalogSourceTable(fileName: String, fileNumber: Int): CsvTableSource = {
    val resourcePath = this.getClass.getResource(getFilePathAsResource(fileNumber, fileName)).getPath
    val builder = CsvTableSource.builder()
    builder.path(resourcePath)
    builder.field("X", Types.STRING)

    if (fileName != tableNameA)
      builder.field("Y", Types.STRING)

    builder.build()
  }


}
