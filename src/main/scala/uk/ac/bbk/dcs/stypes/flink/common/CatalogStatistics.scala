package uk.ac.bbk.dcs.stypes.flink.common


import org.apache.flink.calcite.shaded.com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics

@JsonCreator
case class CatalogStatistics(@JsonProperty("rowCount") rowCount: Long,
                             @JsonProperty("fileCount") fileCount: Int,
                             @JsonProperty("totalSize") totalSize: Long,
                             @JsonProperty("rawDataSize") rawDataSize: Long)
  extends CatalogTableStatistics(rowCount, fileCount, totalSize, rawDataSize)

object CatalogStatistics {
  def formCatalogTableStatistics(s: CatalogTableStatistics): CatalogStatistics = {
    CatalogStatistics(s.getRowCount, s.getFileCount, s.getTotalSize, s.getRawDataSize)
  }

  def toCatalogTableStatistics(s: CatalogStatistics): CatalogTableStatistics = {
    new CatalogTableStatistics(s.rowCount, s.fileCount, s.totalSize, s.rawDataSize)
  }
}