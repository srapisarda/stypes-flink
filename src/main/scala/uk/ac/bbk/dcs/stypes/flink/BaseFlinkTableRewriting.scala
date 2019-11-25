package uk.ac.bbk.dcs.stypes.flink

import java.io.{BufferedReader, IOException, InputStreamReader, OutputStreamWriter}
import java.util.UUID

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.catalog.{Catalog, ConnectorCatalogTable, ObjectPath}
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import uk.ac.bbk.dcs.stypes.flink.common.{Configuration, RewritingEnvironment}

import scala.collection.JavaConverters._
import scala.io.Source


trait BaseFlinkTableRewriting extends BaseFlinkRewriting {
  val catalogName = "S_CAT"
  val databaseName = "default_database"
  private val tableNameS = "S"
  private val tableNameA = "A"
  private val tableNameR = "R"
  private val tableNameB = "B"
  private val tableNameSink1Prefix = s"sink_1"
  private val tableNameSink2Prefix = s"sink_2"
  private val tableNameSinkCountPrefix = s"sink_count"
  private val pathS = new ObjectPath(databaseName, tableNameS)
  private val pathA = new ObjectPath(databaseName, tableNameA)
  private val pathR = new ObjectPath(databaseName, tableNameR)
  private val pathB = new ObjectPath(databaseName, tableNameB)
  val sources: List[ObjectPath] = List(pathS, pathA, pathB, pathR)
  val sinkPrefixes: List[String] = List(tableNameSink1Prefix, tableNameSink2Prefix, tableNameSinkCountPrefix)
  private val isLocalResources = Configuration.getEnvironment == RewritingEnvironment.Local.toString.toLowerCase()
  private val pathToBenchmarkTableNDL_SQL =
    if (isLocalResources)
      "/" + pathToBenchmarkNDL_SQL.replace("src/test/resources/", "")
    else
      pathToBenchmarkNDL_SQL


  import com.google.gson.{Gson, GsonBuilder}

  private val gson: Gson = new GsonBuilder().setPrettyPrinting().create


  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()

  def executeTableRewriting(fileNumber: Int, serial: String, jobName: String, tableEnv: TableEnvironment,
                            tableRewritingEvaluation: (Int, String, TableEnvironment) => Table): Unit = {

    val p1 = tableRewritingEvaluation.apply(fileNumber, jobName, tableEnv)
    val catalog = tableEnv.getCatalog(catalogName)
    if (catalog.isPresent) {
      p1.insertInto(getSinkTableName(tableNameSink1Prefix, catalog.get()))
      val res = p1.select("y.count")
      res.insertInto(getSinkTableName(tableNameSinkCountPrefix, catalog.get()))
    }
    tableEnv.execute("Q27 sql")

  }

  def makeTableEnvironment(fileNumber: Int, jobName: String): TableEnvironment = {
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)
    tableEnv.getConfig // access high-level configuration
      .getConfiguration // set low-level key-value options
      .setString("table.optimizer.join-reorder-enabled", "true")

    val catalog: Catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).orElse(null)

    tableEnv.registerCatalog(catalogName, catalog)
    tableEnv.useCatalog(catalogName)
    tableEnv.useDatabase(databaseName)

    sources
      .foreach(path => {
        catalog.createTable(path,
          ConnectorCatalogTable.source(getExternalCatalogSourceTable(path.getObjectName, fileNumber), false),
          false)
      })

    val uuid = newUUID

    println(s"\n------------> using sink uuid: $uuid\n")

    sinkPrefixes
      .foreach(sinkPrefix => {
        val sinkName = s"${sinkPrefix}_$uuid"
        catalog.createTable(new ObjectPath(databaseName, sinkName),
          ConnectorCatalogTable.sink(getExternalCatalogSinkTable(sinkName, fileNumber, jobName), true),
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
    //    FileSystem.get()

    sources.map(source => {
      val path = new Path(
        if (isLocalResources)
          this.getClass.getResource(getFilePathAsResource(fileNumber, source.getObjectName)).getPath
        else
          getFilePathAsResource(fileNumber, source.getObjectName)
      )

      val fs = FileSystem.getLocalFileSystem
      val filePathStats = new Path(path.toUri.getPath.concat("-stats.json"))
      fs.createRecoverableWriter()
      val statistic =
        if (fs.exists(filePathStats)) {
          readStatisticFromFile(filePathStats)
        }
        else {
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

  private def getResultSinkPath(fileName: String, fileNumber: Int, jobName: String) = {

    val resourcePath =
      if (Configuration.getEnvironment == RewritingEnvironment.Local.toString)
        this.getClass.getResource(getFilePathFolderAsResource).getPath
      else
        getFilePathFolderAsResource

    s"$resourcePath/sink/$fileName-$fileNumber-$jobName"
  }

  private def getExternalCatalogSinkTable(fileName: String, fileNumber: Int, jobName: String): TableSink[Row] = {
    val csvTableSink = new CsvTableSink(getResultSinkPath(fileName, fileNumber, jobName))
    val fieldNames: Array[String] = if (fileName.startsWith(tableNameSinkCountPrefix)) Array("X") else Array("X", "Y")
    val fieldTypes: Array[TypeInformation[_]] = if (fileName.startsWith(tableNameSinkCountPrefix)) Array(Types.LONG) else Array(Types.STRING, Types.STRING)
    csvTableSink.configure(fieldNames, fieldTypes)
  }

  private def getExternalCatalogSourceTable(fileName: String, fileNumber: Int): CsvTableSource = {
    val filePath = getFilePathAsResource(fileNumber, fileName)
    val resourcePath = this.getClass.getResource(filePath).getPath
    val builder = CsvTableSource.builder()
    builder.path(resourcePath)
    builder.field("X", Types.STRING)

    if (!(fileName == tableNameA || fileName == tableNameB))
      builder.field("Y", Types.STRING)

    builder.build()
  }

  private def getTableTabStatistic(path: Path) = {
    val source = FileSystem.get(path.toUri)
    val fileStatus = source.getFileStatus(path)
    val totalSize = fileStatus.getLen
    val file: DataSet[String] = env.readTextFile(path.toUri.getPath)
    file.count()
    val inputStream = source.open(path)
    inputStream.skip(Long.MaxValue)
    val rowCount = inputStream.getPos
    new CatalogTableStatistics(rowCount, 1, totalSize, totalSize * 2)

  }

  private def readStatisticFromFile(path: Path): CatalogTableStatistics = {
    val stream = FileSystem.get(path.toUri).open(path)
    val br = new BufferedReader(new InputStreamReader(stream))
    gson.fromJson(br, classOf[CatalogTableStatistics])
  }

  @throws[IOException]
  private def writeStatisticToFile(statistic: CatalogTableStatistics, path: Path): Unit = {
    val source: FileSystem = FileSystem.get(path.toUri)
    if (!source.exists(path)) {
      val json = gson.toJson(statistic)
      val outputStream = source.create(path, FileSystem.WriteMode.OVERWRITE)
      val outputStreamWriter = new OutputStreamWriter(outputStream, "UTF-8")
      outputStreamWriter.write(json)
      outputStreamWriter.close()
    }
  }

  private def newUUID = UUID.randomUUID().toString.replaceAll("-", "_")

  def getCountFromSink(fileNumber: Int, catalog: Catalog, jobName: String): Int = {
    def sinkTableName = getSinkTableName(tableNameSinkCountPrefix, catalog)

    def source = Source.fromFile(getResultSinkPath(sinkTableName, fileNumber, jobName))

    source.getLines.toList.head.toInt
  }

  def getFilePathAsResource(fileNumber: Int, name: String): String =
    s"$pathToBenchmarkTableNDL_SQL/data/csv/$fileNumber.ttl-$name.csv"

  def getFilePathFolderAsResource: String =
    s"$pathToBenchmarkTableNDL_SQL/data/csv/"
}
