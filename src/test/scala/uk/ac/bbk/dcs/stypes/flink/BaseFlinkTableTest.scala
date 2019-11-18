package uk.ac.bbk.dcs.stypes.flink

import java.util.UUID

import org.apache.calcite.tools.RuleSets
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog.{ConnectorCatalogTable, ObjectPath}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import org.junit.Assert.assertNotNull

trait BaseFlinkTableTest extends BaseFlinkTest {
  private val catalogName = "S_CAT"
  private val databaseName = "default_database"
  private val tableNameS = "S"
  private val tableNameA = "A"
  private val tableNameR = "R"
  private val tableNameB = "B"
  val tableNameSink1 = s"sink_1"
  val tableNameSink2 = s"sink_2"
  val tableNameSinkCount = s"sink_count"
  private val pathS = new ObjectPath(databaseName, tableNameS)
  private val pathA = new ObjectPath(databaseName, tableNameA)
  private val pathR = new ObjectPath(databaseName, tableNameR)
  private val pathB = new ObjectPath(databaseName, tableNameB)
  private val pathSink1 = new ObjectPath(databaseName, tableNameSink1)
  private val pathSink2 = new ObjectPath(databaseName, tableNameSink2)
  private val pathSinkCount = new ObjectPath(databaseName, tableNameSinkCount)

  private val fileNumber = 3

  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()

  val tableEnv: TableEnvironment = TableEnvironment.create(settings)
//  tableEnv.getConfig // access high-level configuration
//    .getConfiguration // set low-level key-value options
//    .setString("table.optimizer.join-reorder-enabled", "true")

  val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).orElse(null)

  tableEnv.registerCatalog(catalogName, catalog)
  tableEnv.useCatalog(catalogName)
  tableEnv.useDatabase(databaseName)

  assertNotNull(catalog)
  catalog.createTable(pathS,
    ConnectorCatalogTable.source(getExternalCatalogSourceTable(tableNameS, fileNumber), true),
    false)
  catalog.createTable(pathA,
    ConnectorCatalogTable.source(getExternalCatalogSourceTable(tableNameA, fileNumber), true),
    false)
  catalog.createTable(pathR,
    ConnectorCatalogTable.source(getExternalCatalogSourceTable(tableNameR, fileNumber), true),
    false)
  catalog.createTable(pathB,
    ConnectorCatalogTable.source(getExternalCatalogSourceTable(tableNameB, fileNumber), true),
    false)

  catalog.createTable(pathSink1,
    ConnectorCatalogTable.sink(getExternalCatalogSinkTable(tableNameSink1, fileNumber), true),
    false
  )
  catalog.createTable(pathSink2,
    ConnectorCatalogTable.sink(getExternalCatalogSinkTable(tableNameSink2, fileNumber), true),
    false
  )

  catalog.createTable(pathSinkCount,
    ConnectorCatalogTable.sink(getExternalCatalogSinkTable(tableNameSinkCount, fileNumber), true),
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

  // change calcite configuration
//  val calciteConfig: CalciteConfig = new CalciteConfigBuilder()
//    .addDecoRuleSet(RuleSets.ofList(DataSetJoinRule.INSTANCE))
//    .addDecoRuleSet(RuleSets.ofList(DataSetUnionRule.INSTANCE,
//      DataStreamRetractionRules.ACCMODE_INSTANCE)
//    )
//    .build()
//
//  tableEnv.getConfig.setPlannerConfig(calciteConfig)


  private def newUUID = UUID.randomUUID().toString.replaceAll("-", "_")

  private def cleanDir(path: String) = {
    val dir = FileUtils.getFile(path)
    if (dir.exists() && dir.isDirectory)
      FileUtils.cleanDirectory(dir)
  }

  def cleanSink() = {
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
    val fieldNames: Array[String] = if(fileName == tableNameSinkCount) Array("X") else Array("X", "Y")
    val fieldTypes: Array[TypeInformation[_]] = if(fileName == tableNameSinkCount) Array(Types.LONG) else Array(Types.STRING, Types.STRING)
    csvTableSink.configure(fieldNames, fieldTypes)
  }

  private def getExternalCatalogSourceTable(fileName: String, fileNumber: Int): CsvTableSource = {
    val resourcePath = this.getClass.getResource(getFilePathAsResource(fileNumber, fileName)).getPath
    val builder = CsvTableSource.builder()
    builder.path(resourcePath)
    builder.field("X", Types.STRING)

    if (!(fileName == tableNameA || fileName == tableNameB))
      builder.field("Y", Types.STRING)

    builder.build()
  }
}
