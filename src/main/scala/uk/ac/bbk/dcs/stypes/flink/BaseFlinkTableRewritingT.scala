package uk.ac.bbk.dcs.stypes.flink

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, ExplainDetail, Table, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.catalog.{Catalog, ConnectorCatalogTable, ObjectPath}
import org.apache.flink.table.plan.rules.dataSet.{DataSetJoinRule, DataSetScanRule, DataSetUnionRule}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.rules.logical.FlinkFilterJoinRule
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.types.Row
import uk.ac.bbk.dcs.stypes.flink.common.{CatalogStatistics, Configuration, RewritingEnvironment}

import java.io.{BufferedReader, File, FileInputStream, IOException, InputStreamReader, OutputStreamWriter}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.io.Source

trait BaseFlinkTableRewritingT extends BaseFlinkRewriting {
  val catalogName = "S_CAT"
  val databaseName = "default_database"

  // cheking
  //  val catalog01 = parseYaml("catalog-01.yaml")

  private val tableNameEmployee = "employee"
  private val tableNameProject = "project"
  private val tableNameEmployeeProject = "employee_project"

  private val tableStructure = Map(
    tableNameEmployee -> List("id", "name", "manager_id"),
    tableNameEmployeeProject -> List("employee_id", "project_id", "until"),
    tableNameProject -> List("id", "name")
  )

  private val tableNameSink1Prefix = s"sink_1"
  private val tableNameSink2Prefix = s"sink_2"
  private val tableNameSinkCountPrefix = s"sink_count"

  private val pathEmployee = new ObjectPath(databaseName, tableNameEmployee)
  private val pathProject = new ObjectPath(databaseName, tableNameProject)
  private val pathEmployeeProject = new ObjectPath(databaseName, tableNameEmployeeProject)

  val sources: List[ObjectPath] = List(pathProject, pathEmployee, pathEmployeeProject)
  val sinkPrefixes: List[String] = List(tableNameSink1Prefix, tableNameSink2Prefix, tableNameSinkCountPrefix)
  private val isLocalResources = Configuration.getEnvironment == RewritingEnvironment.Local.toString.toLowerCase()

  private val pathToBenchmarkTableNDL_SQL = //Configuration.getDataPath
    if (isLocalResources)
      "/" + pathToBenchmarkNDL_SQL.replace("src/test/resources/", "")
    else
      Configuration.getDataPath

  def parseYaml(file: String) = {
    val yaml = new Yaml()
    val source = new FileInputStream(new File((s"src/main/resources/$file")))
    val ret = yaml.load(source).asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    ret
  }


  private val defaultCatalogStatistics: Map[(Int, ObjectPath), CatalogStatistics] = Map(
    (1, pathEmployee) -> CatalogStatistics(3,1,46,92),
    (1, pathEmployeeProject) -> CatalogStatistics(2,1,41,82),
    (1, pathProject) -> CatalogStatistics(2,1,31,62),

    (2, pathEmployee) -> CatalogStatistics(100,1,1660,3320),
    (2, pathEmployeeProject) -> CatalogStatistics(100,1,1300,2600),
    (2, pathProject) -> CatalogStatistics(20,1,300,600),

    (3, pathEmployee) -> CatalogStatistics(300,1,5460,10920),
    (3, pathEmployeeProject) -> CatalogStatistics(300,1,4460,8920),
    (3, pathProject) -> CatalogStatistics(60,1,940,1880),

    (4, pathEmployee) -> CatalogStatistics(500,1,9540,19080),
    (4, pathEmployeeProject) -> CatalogStatistics(500,1,7530,15060),
    (4, pathProject) -> CatalogStatistics(100,1,1580,3160),

    (5, pathEmployee) -> CatalogStatistics(800,1,15450,30900),
    (5, pathEmployeeProject) -> CatalogStatistics(5800,1,12500,25000),
    (5, pathProject) -> CatalogStatistics(160,1,2660,5320),

    (6, pathEmployee) -> CatalogStatistics(1300,1,26170,52340),
    (6, pathEmployeeProject) -> CatalogStatistics(1300,1,20800,41600),
    (6, pathProject) -> CatalogStatistics(260,1,4460,8920),

    (7, pathEmployee) -> CatalogStatistics(2100,1,44250,88500),
    (7, pathEmployeeProject) -> CatalogStatistics(2100,1,45638,91276),
    (7, pathProject) -> CatalogStatistics(420,1,7340,14680),

    (8, pathEmployee) -> CatalogStatistics(3400,1,73630,147260),
    (8, pathEmployeeProject) -> CatalogStatistics(3400,1,57640,115280),
    (8, pathProject) -> CatalogStatistics(680,1,12020,24040),

    (9, pathEmployee) -> CatalogStatistics(5500,1,121090,242180),
    (8, pathEmployeeProject) -> CatalogStatistics(5500,1,94720,189440),
    (8, pathProject) -> CatalogStatistics(1100,1,19780,39560),

  )

  val objectMapper: ObjectMapper = new databind.ObjectMapper()

  private val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()

  def executeTableRewriting(fileNumber: Int, serial: String, jobName: String, tableEnv: TableEnvironment,
                            tableRewritingEvaluation: (Int, String, TableEnvironment) => Table): Unit = {

    val p1 = tableRewritingEvaluation.apply(fileNumber, jobName, tableEnv)
    val catalog = tableEnv.getCatalog(catalogName)

    // print the estimated cost
    println(p1.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE))
    if (catalog.isPresent) {
      p1.executeInsert(getSinkTableName(tableNameSink1Prefix, catalog.get()))
    }
  }

  def makeTableEnvironment(fileNumber: Int, jobName: String, optimisationEnabled: Boolean = true,
                           statistics: Map[(Int, ObjectPath), CatalogStatistics] = defaultCatalogStatistics): TableEnvironment = {
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)
    tableEnv.getConfig // access high-level configuration
      .getConfiguration // set low-level key-value options
      .setString("table.optimizer.join-reorder-enabled", if (optimisationEnabled) "true" else "false")

    println(s"\n-----------+> is optimised: $optimisationEnabled\n")

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
      .addDecoRuleSet(RuleSets.ofList(
        DataSetJoinRule.INSTANCE,
        DataSetUnionRule.INSTANCE,
        DataSetScanRule.INSTANCE,
        DataStreamRetractionRules.ACCMODE_INSTANCE,
        FlinkFilterJoinRule.FILTER_ON_JOIN
        //        JoinCommuteRule.INSTANCE,
        //        JoinAssociateRule.INSTANCE,
        //        JoinToMultiJoinRule.INSTANCE,
        //        LoptOptimizeJoinRule.INSTANCE,
        //        MultiJoinOptimizeBushyRule.INSTANCE
      ))
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
    val fieldNames: Array[String] =
      if (fileName.startsWith(tableNameSinkCountPrefix)) Array("x")
      else Array("x1", "x2")

    val fieldTypes: Array[TypeInformation[_]] =
      if (fileName.startsWith(tableNameSinkCountPrefix)) Array(Types.LONG)
      else Array(Types.STRING, Types.STRING)

    csvTableSink.configure(fieldNames, fieldTypes)
  }

  private def getExternalCatalogSourceTable(fileName: String, fileNumber: Int): TableSource[Row] = {
    val filePath = getFilePathAsResource(fileNumber, fileName)
    val resourcePath = filePath // this.getClass.getResource(filePath).getPath
    val builder = CsvTableSource.builder()
    builder.path(resourcePath)
    tableStructure(fileName).foreach(fieldName => builder.field(fieldName, DataTypes.STRING))
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
    s"$pathToBenchmarkTableNDL_SQL/data/csv/$fileNumber-$name.csv"

  def getFilePathFolderAsResource: String =
    s"$pathToBenchmarkTableNDL_SQL/data/csv/"
}
