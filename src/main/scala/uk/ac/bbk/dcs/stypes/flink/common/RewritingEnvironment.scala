package uk.ac.bbk.dcs.stypes.flink.common

object RewritingEnvironment extends Enumeration {
  type Environment = Value
  val Local, Hadoop, Hive, Unknown = Value

  def withNameWithDefault(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Unknown)

}
