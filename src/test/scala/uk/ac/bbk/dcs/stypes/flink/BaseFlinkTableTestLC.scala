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

import org.apache.calcite.tools.RuleSets
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper
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
import uk.ac.bbk.dcs.stypes.flink.common.CatalogStatistics

import java.io.{FileReader, IOException}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.io.Source


trait BaseFlinkTableTestLC extends BaseFlinkTest {
  val catalogName = "S_CAT"
  val databaseName = "default_database"
  private val tableNameS = "s"
  private val tableNameA = "a"
  private val tableNameR = "r"
  private val tableNameB = "b"
  val tableNameSink1Prefix = s"sink_1"
  val tableNameSink2Prefix = s"sink_2"
  val tableNameSinkCountPrefix = s"sink_count"
  private val pathS = new ObjectPath(databaseName, tableNameS)
  private val pathA = new ObjectPath(databaseName, tableNameA)
  private val pathR = new ObjectPath(databaseName, tableNameR)
  private val pathB = new ObjectPath(databaseName, tableNameB)
  val sources: List[ObjectPath] = List(pathS, pathA, pathB, pathR)
  val sinkPrefixes: List[String] = List(tableNameSink1Prefix, tableNameSink2Prefix, tableNameSinkCountPrefix)

  val objectMapper: ObjectMapper = new databind.ObjectMapper()

  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()


  //  val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
  //  batchTableEnv.registerCatalog(catalogName, catalog)
  //  batchTableEnv.useCatalog(catalogName)
  //  batchTableEnv.useDatabase(databaseName)
  //  batchTableEnv.getConfig
  //    .getConfiguration // set low-level key-value options
  //    .setString("table.optimizer.join-reorder-enabled", "true")


  def makeTableEnvironment(fileNumber: Int, jobName: String): TableEnvironment = {
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)
   val configuration = tableEnv.getConfig // access high-level configuration
      .getConfiguration // set low-level key-value options

      configuration.setString("table.optimizer.join-reorder-enabled", "true")
      configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
      configuration.setString("table.exec.mini-batch.size", "5000")

    val catalog: Catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).orElse(null)

    tableEnv.registerCatalog(catalogName, catalog)
    tableEnv.useCatalog(catalogName)
    tableEnv.useDatabase(databaseName)

    assertNotNull(catalog)

    sources
      .foreach(path => {
        catalog.createTable(path,
          ConnectorCatalogTable.source[Row](getExternalCatalogSourceTable(path.getObjectName, fileNumber), false),
          false)
      })

    val uuid = newUUID

    println(s"\n------------> using sink uuid: $uuid\n")

    sinkPrefixes
      .foreach(sinkPrefix => {
        val sinkName = s"${sinkPrefix}_$uuid"
        catalog.createTable(new ObjectPath(databaseName, sinkName),
          ConnectorCatalogTable.sink[Row](getExternalCatalogSinkTable(sinkName, fileNumber, jobName), true),
          false
        )
      })
    addStatisticToCatalog(fileNumber, catalog)
    changeCalciteConfig(tableEnv)
    tableEnv
  }

  def getSinkTableName(sinkTableNamePrefix: String, catalog: Catalog): String =
    catalog.listTables(databaseName).asScala.find(_.startsWith(sinkTableNamePrefix)).last

  def addStatisticToCatalog(fileNumber: Int, catalog: Catalog): Unit = {
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

      catalog.alterTableStatistics(source, statistic, false)
    })
  }

  def changeCalciteConfig(tableEnvironment: TableEnvironment) = {
    //     change calcite configuration
    val calciteConfig: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(DataSetJoinRule.INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(DataSetUnionRule.INSTANCE,
        DataStreamRetractionRules.ACCMODE_INSTANCE)
      )
      .build()

    tableEnvironment.getConfig.setPlannerConfig(calciteConfig)
  }

  private def cleanDir(path: String) = {
    val dir = FileUtils.getFile(path)
    if (dir.exists() && dir.isDirectory)
      FileUtils.cleanDirectory(dir)
  }

  private def cleanSink() = {
    val resourcePath = this.getClass.getResource(getFilePathFolderAsResource).getPath
    cleanDir(s"$resourcePath/sink")
  }

  private def getResultSinkPath(fileName: String, fileNumber: Int, jobName: String) = {
    val resourcePath = this.getClass.getResource(getFilePathFolderAsResource).getPath
    s"$resourcePath/sink/$fileName-$fileNumber-$jobName"
  }

  private def getExternalCatalogSinkTable(fileName: String, fileNumber: Int, jobName: String): TableSink[Row] = {
    val csvTableSink = new CsvTableSink(getResultSinkPath(fileName, fileNumber, jobName))
    val fieldNames: Array[String] = if (fileName.startsWith(tableNameSinkCountPrefix)) Array("X") else Array("X", "Y")
    val fieldTypes: Array[TypeInformation[_]] = if (fileName.startsWith(tableNameSinkCountPrefix)) Array(Types.LONG) else Array(Types.STRING, Types.STRING)
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
    CatalogStatistics(rowCount, 1, totalSize, totalSize * 2)
  }

  private def readStatisticFromFile(path: Path): CatalogTableStatistics = {
    objectMapper.readValue(new FileReader(path.toFile), classOf[CatalogStatistics])
  }

  @throws[IOException]
  private def writeStatisticToFile(statistic: CatalogStatistics, path: Path): Unit = {
    if (!Files.exists(path)) {
      val json = objectMapper.writeValueAsString(statistic)
      FileUtils.write(path.toFile, json)
    }
  }

  private def newUUID = UUID.randomUUID().toString.replaceAll("-", "_")

  def getCountFromSink(fileNumber: Int, catalog: Catalog, jobName: String): Int = {
    def sinkTableName = getSinkTableName(tableNameSinkCountPrefix, catalog)

    def source = Source.fromFile(getResultSinkPath(sinkTableName, fileNumber, jobName))

    source.getLines.toList.head.toInt
  }

}
