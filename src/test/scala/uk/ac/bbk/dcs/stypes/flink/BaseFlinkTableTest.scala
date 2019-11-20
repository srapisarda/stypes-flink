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

import java.io.{FileReader, IOException}
import java.nio.file.{Files, Path, Paths}

import org.apache.calcite.tools.RuleSets
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.catalog.{Catalog, ConnectorCatalogTable, ObjectPath}
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import org.junit.Assert.assertNotNull

import scala.io.Source

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
  val sources: List[ObjectPath] = List(pathS, pathA, pathB, pathR)
  val sinks: List[ObjectPath] = List(pathSink1, pathSink2, pathSinkCount)

  import com.google.gson.{Gson, GsonBuilder}

  private val gson: Gson = new GsonBuilder().setPrettyPrinting().create


  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()

  val tableEnv: TableEnvironment = TableEnvironment.create(settings)
  tableEnv.getConfig // access high-level configuration
    .getConfiguration // set low-level key-value options
    .setString("table.optimizer.join-reorder-enabled", "true")

  val catalog: Catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).orElse(null)

  tableEnv.registerCatalog(catalogName, catalog)
  tableEnv.useCatalog(catalogName)
  tableEnv.useDatabase(databaseName)

  def makeCatalog(fileNumber: Int) {
    assertNotNull(catalog)
    sources
      .foreach(path => catalog.createTable(path,
        ConnectorCatalogTable.source(getExternalCatalogSourceTable(path.getObjectName, fileNumber), true),
        false))
    sinks
      .foreach(sink => catalog.createTable(sink,
        ConnectorCatalogTable.sink(getExternalCatalogSinkTable(sink.getObjectName, fileNumber), true),
        false
      ))
    addStatistic(fileNumber)
    changeCalciteConfig()
  }


  def addStatistic(fileNumber: Int) = {
    val filePaths = sources.map(source => {
      val path = Paths.get(this.getClass.getResource(getFilePathAsResource(fileNumber, source.getObjectName)).getPath)

      val filePathStats = Paths.get(path.toUri.getPath.concat("-stats.json"))

      val statistic =
        if (filePathStats.toFile.exists()) {
          readStatisticFromFile(filePathStats)
        } else {
          val statistics = getTableTabStatistic(path)
          writeStatisticToFile(statistics, filePathStats)
          statistics
        }

      addStatisticToCatalog(statistic, source)
    })
  }

  def addStatisticToCatalog(statistics: CatalogTableStatistics, op: ObjectPath) = {
    catalog.alterTableStatistics(op, statistics, false)
  }

  def changeCalciteConfig() = {
    //     change calcite configuration
    val calciteConfig: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(DataSetJoinRule.INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(DataSetUnionRule.INSTANCE,
        DataStreamRetractionRules.ACCMODE_INSTANCE)
      )
      .build()

    tableEnv.getConfig.setPlannerConfig(calciteConfig)
  }

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

  private def getExternalCatalogSinkTable(fileName: String, fileNumber: Int): TableSink[Row] = {
    val csvTableSink = new CsvTableSink(getResultSinkPath(fileName, fileNumber))
    val fieldNames: Array[String] = if (fileName == tableNameSinkCount) Array("X") else Array("X", "Y")
    val fieldTypes: Array[TypeInformation[_]] = if (fileName == tableNameSinkCount) Array(Types.LONG) else Array(Types.STRING, Types.STRING)
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

  private def getTableTabStatistic(path: Path) = {
    val source = Source.fromFile(path.toUri)
    val totalSize = path.toFile.length()
    val rowCount = source.getLines().length
    new CatalogTableStatistics(rowCount, 1, totalSize, totalSize * 2)
  }

  private def readStatisticFromFile(path: Path): CatalogTableStatistics = {
    gson.fromJson(new FileReader(path.toFile), classOf[CatalogTableStatistics])
  }

  @throws[IOException]
  private def writeStatisticToFile(statistic: CatalogTableStatistics, path: Path): Unit = {
    if (!Files.exists(path)) {
      val json = gson.toJson(statistic)
      FileUtils.write(path.toFile, json)
    }
  }

}
