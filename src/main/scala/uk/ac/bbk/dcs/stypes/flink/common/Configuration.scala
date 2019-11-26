package uk.ac.bbk.dcs.stypes.flink.common

import com.typesafe.config.{Config, ConfigFactory}

object Configuration {
  val instance: Config = ConfigFactory.load()

  def getEnvironment: String = {
    instance.getString("rewriting.environment.type")
  }

  def getDataPath = instance.getString("rewriting.pathToBenchmarkNDL")
}
