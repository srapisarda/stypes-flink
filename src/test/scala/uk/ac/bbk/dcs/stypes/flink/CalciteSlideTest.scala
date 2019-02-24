package uk.ac.bbk.dcs.stypes.flink

import java.sql.DriverManager
import java.util.Properties

import com.google.common.collect.ImmutableList
import org.apache.calcite.adapter.jdbc.{JdbcConvention, JdbcSchema}
import org.apache.calcite.config.CalciteConnectionConfigImpl
import org.apache.calcite.jdbc.{CalciteConnection, CalciteSchema}
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.rules.{FilterMergeRule, FilterProjectTransposeRule, LoptOptimizeJoinRule, ProjectMergeRule}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.Schemas
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlConformanceEnum, SqlValidatorUtil}
import org.apache.calcite.sql2rel.{SqlToRelConverter, StandardConvertletTable}
import org.apache.calcite.tools.{Frameworks, Programs}
import org.apache.calcite.util.Sources
import org.scalatest.FunSpec

import scala.collection.JavaConverters._

class CalciteSlideTest extends FunSpec {
  private val model: String = "src/test/resources/benchmark/Lines/data/model-mysql"
  private val connection = getConnection(model)
  private val calciteConnection = connection.unwrap(classOf[CalciteConnection])
  private val typeFactory = createTypeFactory()

  private def getConnection(model: String) = {
    val info = new Properties
    info.put("model", jsonPath(model))
    DriverManager.getConnection("jdbc:calcite:", info)
  }

  private def createTypeFactory() = {
    calciteConnection.getTypeFactory
  }

  private def createCatalogReader() = {
    val calciteConnectionConfig = new CalciteConnectionConfigImpl(connection.getClientInfo)
    val rootSchema: CalciteSchema = CalciteSchema.from(calciteConnection.getRootSchema)

    new CalciteCatalogReader(rootSchema,
      List(calciteConnection.getSchema).asJava,
      typeFactory,
      calciteConnectionConfig)
  }

  private def createRexBuilder() = {
    new RexBuilder(calciteConnection.getTypeFactory)
  }

  private def jsonPath(model: String) =
    resourcePath(model + ".json")

  private def resourcePath(path: String) =
    Sources.of(new java.io.File(path)).file.getAbsolutePath

  private def getFrameworkConfig() = {
    val builder =
      Frameworks.newConfigBuilder.defaultSchema(calciteConnection.getRootSchema)
        .parserConfig(SqlParser.configBuilder.setCaseSensitive(false).build)

    builder.build()
  }

  it("shoild") {
    val sql = " SELECT COUNT(*) as NUM " +
      "FROM TTLA_ONE A  " +
      "INNER JOIN TTLR_ONE B1 ON A.X = B1.X " +
      "INNER JOIN TTLR_ONE B2 ON B2.X = B1.X " +
      "INNER JOIN EMPTY_T C1 ON C1.X = B2.Y " +
      "INNER JOIN EMPTY_T C2 ON C2.X = C2.X "

    // Parse the query
    val parserConfig = SqlParser.configBuilder().build()
    val parser = SqlParser.create(sql, parserConfig)
    val sqlNode = parser.parseStmt()

    // Validate query
    val catalogReader = createCatalogReader()
    val validator = SqlValidatorUtil.newValidator(
      SqlStdOperatorTable.instance(), catalogReader, typeFactory, SqlConformanceEnum.DEFAULT)
    val validateSqlNode = validator.validate(sqlNode)

    // convert SqlNode to RelNode
    val rexBuilder = createRexBuilder()

    val planner = new HepPlanner(HepProgram.builder()
      .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
      .addRuleInstance(ProjectMergeRule.INSTANCE)
      .addRuleInstance(FilterMergeRule.INSTANCE)
      .addRuleInstance(LoptOptimizeJoinRule.INSTANCE)
      build())
    val cluster = RelOptCluster.create(planner, rexBuilder)
    val convertletTable = StandardConvertletTable.INSTANCE

    val configBuilder =
      SqlToRelConverter.configBuilder().withTrimUnusedFields(true)

    //    val plannerImpl: PlannerImpl  = new PlannerImpl(getFrameworkConfig)
    //    val viewExpander:PlannerImpl#ViewExpanderImpl = new  plannerImpl.ViewExpanderImpl()

    val sqlToRelConverter =
      new SqlToRelConverter(null, validator, catalogReader, cluster, convertletTable)
    val root = sqlToRelConverter.convertQuery(validateSqlNode, false, true)


    val program = Programs.ofRules(
      FilterProjectTransposeRule.INSTANCE,
      ProjectMergeRule.INSTANCE,
      FilterMergeRule.INSTANCE,
      LoptOptimizeJoinRule.INSTANCE
    )



    val expressionName = "mysql"
    val expression = Schemas.subSchemaExpression(calciteConnection.getRootSchema, expressionName, classOf[JdbcSchema])
    val convention = JdbcConvention.of(SqlDialect.DatabaseProduct.MSSQL.getDialect, expression, expressionName)
    val trailSet = planner.emptyTraitSet().replace(convention) // .replace( Convention)

    val optimised = program.run(planner, root.rel,  trailSet, ImmutableList.of(), ImmutableList.of())

    println(optimised.toString)
  }

}
