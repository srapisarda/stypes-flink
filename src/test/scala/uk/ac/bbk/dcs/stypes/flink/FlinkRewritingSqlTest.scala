package uk.ac.bbk.dcs.stypes.flink

import org.scalatest.FunSpec

import scala.io.Source

class FlinkRewritingSqlTest extends FunSpec {

  describe( "Flink rewriting using SQL") {
    it("should run a job and execute the sql") {
      val source = Source.fromFile("src/test/resources/sql/q22_flatten_p3_05.sql")
      val sql = source.getLines().mkString("\n")
      source.close()

      FlinkRewritingSql.run(3, "test", true, sql )
    }
  }
}
