package uk.ac.bbk.dcs.stypes.flink.common

import org.scalatest.{FunSpec, Matchers}
import uk.ac.bbk.dcs.stypes.flink.common.Configuration.getEnvironment

class ConfigurationTest extends FunSpec with Matchers {

  describe("Configuration Test") {
    it("should load the configuration") {
      assert(Configuration.instance!=null)
    }

    it("should assert a local environment for the re-writer") {
      assert(getEnvironment=="local")
    }

  }
}
