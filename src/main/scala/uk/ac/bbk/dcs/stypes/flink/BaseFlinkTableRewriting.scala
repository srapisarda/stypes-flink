package uk.ac.bbk.dcs.stypes.flink

import java.io.{BufferedReader, IOException, InputStreamReader, OutputStreamWriter}
import java.util.UUID

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.catalog.{Catalog, ConnectorCatalogTable, ObjectPath}
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.types.Row
import uk.ac.bbk.dcs.stypes.flink.common.{CatalogStatistics, Configuration, RewritingEnvironment}

import scala.collection.JavaConverters._
import scala.io.Source


trait BaseFlinkTableRewriting extends BaseFlinkRewriting {
  val catalogName = "S_CAT"
  val databaseName = "default_database"
  private val tableNameS = "S"
  private val tableNameA = "A"
  private val tableNameR = "R"
  private val tableNameB = "B"
  private val tableNameT = "T"
  private val tableNameSink1Prefix = s"sink_1"
  private val tableNameSink2Prefix = s"sink_2"
  private val tableNameSinkCountPrefix = s"sink_count"
  private val pathS = new ObjectPath(databaseName, tableNameS)
  private val pathA = new ObjectPath(databaseName, tableNameA)
  private val pathR = new ObjectPath(databaseName, tableNameR)
  private val pathB = new ObjectPath(databaseName, tableNameB)
  private val pathT = new ObjectPath(databaseName, tableNameT)
  val sources: List[ObjectPath] = List(pathS, pathA, pathB, pathR, pathT)
  val sinkPrefixes: List[String] = List(tableNameSink1Prefix, tableNameSink2Prefix, tableNameSinkCountPrefix)
  private val isLocalResources = Configuration.getEnvironment == RewritingEnvironment.Local.toString.toLowerCase()
  private val pathToBenchmarkTableNDL_SQL = Configuration.getDataPath
  //    if (isLocalResources)
  //      "/" + pathToBenchmarkNDL_SQL.replace("src/test/resources/", "")
  //    else
  //      pathToBenchmarkNDL_SQL

  private val defaultCatalogStatistics: Map[(Int, ObjectPath), CatalogStatistics] = Map(
    (1, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (1, pathA) -> CatalogStatistics(59, 1, 232, 464),
    (1, pathB) -> CatalogStatistics(48, 1, 183, 366),
    (1, pathR) -> CatalogStatistics(61390, 1, 477853, 955706),
    (1, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (2, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (2, pathA) -> CatalogStatistics(22, 1, 107, 214),
    (2, pathB) -> CatalogStatistics(31, 1, 150, 300),
    (2, pathR) -> CatalogStatistics(64103, 1, 612911, 1225822),
    (2, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (3, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (3, pathA) -> CatalogStatistics(57, 1, 277, 554),
    (3, pathB) -> CatalogStatistics(47, 1, 226, 452),
    (3, pathR) -> CatalogStatistics(256699, 1, 2510481, 5020962),
    (3, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (4, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (4, pathA) -> CatalogStatistics(248, 1, 1353, 2706),
    (4, pathB) -> CatalogStatistics(253, 1, 1383, 2766),
    (4, pathR) -> CatalogStatistics(1026526, 1, 11178724, 22357448),
    (4, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (5, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (5, pathA) -> CatalogStatistics(336, 1, 1892, 3784),
    (5, pathB) -> CatalogStatistics(357, 1, 2013, 4026),
    (5, pathR) -> CatalogStatistics(2307054, 1, 25975560, 51951120),
    (5, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (6, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (6, pathA) -> CatalogStatistics(492, 1, 2807, 5614),
    (6, pathB) -> CatalogStatistics(463, 1, 2654, 5308),
    (6, pathR) -> CatalogStatistics(4101642, 1, 46940747, 93881494),
    (6, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (7, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (7, pathA) -> CatalogStatistics(600, 1, 3472, 6944),
    (7, pathB) -> CatalogStatistics(565, 1, 2654, 5308),
    (7, pathR) -> CatalogStatistics(6410095, 1, 46940747, 93881494),
    (7, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (8, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (8, pathA) -> CatalogStatistics(492, 1, 2807, 5614),
    (8, pathB) -> CatalogStatistics(463, 1, 2654, 5308),
    (8, pathR) -> CatalogStatistics(4101642, 1, 46940747, 93881494),
    (8, pathT) -> CatalogStatistics(1, 1, 3, 3),

    (9, pathS) -> CatalogStatistics(0, 1, 0, 0),
    (9, pathA) -> CatalogStatistics(492, 1, 2807, 5614),
    (9, pathB) -> CatalogStatistics(463, 1, 2654, 5308),
    (9, pathR) -> CatalogStatistics(4101642, 1, 46940747, 93881494),
    (9, pathT) -> CatalogStatistics(1, 1, 3, 3)
  )

  import com.google.gson.{Gson, GsonBuilder}

  val objectMapper: ObjectMapper = new databind.ObjectMapper()

  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()

  def executeTableRewriting(fileNumber: Int, serial: String, jobName: String, tableEnv: TableEnvironment,
                            tableRewritingEvaluation: (Int, String, TableEnvironment) => Table): Unit = {

    val p1 = tableRewritingEvaluation.apply(fileNumber, jobName, tableEnv)
    val catalog = tableEnv.getCatalog(catalogName)

    println(tableEnv.explain(p1))

    if (catalog.isPresent) {
      p1.insertInto(getSinkTableName(tableNameSink1Prefix, catalog.get()))
      val res = p1.select("y.count")
      res.insertInto(getSinkTableName(tableNameSinkCountPrefix, catalog.get()))
    }
    tableEnv.execute(s"$jobName as ")

  }

  def makeTableEnvironment(fileNumber: Int, jobName: String, optimisationEnabled: Boolean = true,
                           statistics: Map[(Int, ObjectPath), CatalogStatistics] = defaultCatalogStatistics): TableEnvironment = {
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)
    tableEnv.getConfig // access high-level configuration
      .getConfiguration // set low-level key-value options
      .setString("table.optimizer.join-reorder-enabled", if (optimisationEnabled) "true" else "false")

    val catalog: Catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).orElse(null)

    tableEnv.registerCatalog(catalogName, catalog)
    tableEnv.useCatalog(catalogName)
    tableEnv.useDatabase(databaseName)

    sources
      .foreach(path => {
        catalog.createTable(path,
          ConnectorCatalogTable.source[Row](getExternalCatalogSourceTable(path.getObjectName, fileNumber), true),
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

    if (optimisationEnabled) {
      addStatisticToCatalog(fileNumber, catalog, statistics)
    }
    changeCalciteConfig(tableEnv)
    tableEnv
  }

  def getSinkTableName(sinkTableNamePrefix: String, catalog: Catalog): String =
    catalog.listTables(databaseName).asScala.find(_.startsWith(sinkTableNamePrefix)).last

  def addStatisticToCatalog(fileNumber: Int, catalog: Catalog, catalogStatistics: Map[(Int, ObjectPath), CatalogStatistics]): Unit = {
    //    FileSystem.get()

    sources.foreach(source => {
      val key = (fileNumber, source)
      if (catalogStatistics.contains(key)) {
        val msg = s"Adding statistics for table ${source.getObjectName} number $fileNumber "
        log.info(msg)
        println(msg)
        catalog.alterTableStatistics(source, catalogStatistics(key), false)
      }
      //      val path = new Path(
      //        if (isLocalResources)
      //          this.getClass.getResource(getFilePathAsResource(fileNumber, source.getObjectName)).getPath
      //        else
      //          getFilePathAsResource(fileNumber, source.getObjectName)
      //      )


      //        val stats = env.readTextFile( path.toUri.getPath.concat("-stats.json"))

      //      val fs = FileSystem.getLocalFileSystem
      //      val filePathStats = new Path(path.toUri.getPath.concat("-stats.json"))
      //      val statistics =
      //        if (fs.exists(filePathStats)) {
      //          readStatisticFromFile(filePathStats)
      //        }
      //        else {
      //          val statistics = getTableTabStatistic(path)
      //          writeStatisticToFile(statistics, filePathStats)
      //          statistics
      //        }
      //
      //      val statistic = getTableTabStatistic(path)
      //      catalog.alterTableStatistics(source, statistics, false)
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
    //      if (Configuration.getEnvironment == RewritingEnvironment.Local.toString)
    //        this.getClass.getResource(getFilePathFolderAsResource).getPath
    //      else
      getFilePathFolderAsResource

    s"$resourcePath/sink/$fileName-$fileNumber-$jobName"
  }

  private def getExternalCatalogSinkTable(fileName: String, fileNumber: Int, jobName: String): TableSink[Row] = {
    val csvTableSink = new CsvTableSink(getResultSinkPath(fileName, fileNumber, jobName))
    val fieldNames: Array[String] = if (fileName.startsWith(tableNameSinkCountPrefix)) Array("X") else Array("X", "Y")
    val fieldTypes: Array[TypeInformation[_]] = if (fileName.startsWith(tableNameSinkCountPrefix)) Array(Types.LONG) else Array(Types.STRING, Types.STRING)
    csvTableSink.configure(fieldNames, fieldTypes)
  }

  private def getExternalCatalogSourceTable(fileName: String, fileNumber: Int): TableSource[Row] = {
    val filePath = getFilePathAsResource(fileNumber, fileName)
    val resourcePath = filePath // this.getClass.getResource(filePath).getPath
    val builder = CsvTableSource.builder()
    builder.path(resourcePath)
    builder.field("X", DataTypes.STRING)

    if (!(fileName == tableNameA || fileName == tableNameB))
      builder.field("Y", DataTypes.STRING)

    builder.build()
  }

  private def getStream(path: Path) = {
    FileSystem.get(path.toUri).open(path)
  }

  private def getTableTabStatistic(path: Path) = {
    val source = FileSystem.get(path.toUri)
    val fileStatus = source.getFileStatus(path)
    val totalSize = fileStatus.getLen
    val stream = getStream(path)
    val br = new BufferedReader(new InputStreamReader(stream))
    val rowCount = br.lines().count()
    stream.close()
    new CatalogTableStatistics(rowCount, 1, totalSize, totalSize * 2)
  }

  private def readStatisticFromFile(path: Path): CatalogTableStatistics = {
    val stream = getStream(path)
    val br = new BufferedReader(new InputStreamReader(stream))
    val statistics = objectMapper.readValue(br, classOf[CatalogStatistics])
    stream.close()
    statistics
  }

  @throws[IOException]
  private def writeStatisticToFile(statistic: CatalogTableStatistics, path: Path): Unit = {
    val source: FileSystem = FileSystem.get(path.toUri)
    if (!source.exists(path)) {
      val json = objectMapper.writeValueAsString(statistic)
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
