package com.deloitte.missiongraph.domain

import com.deloitte.missiongraph.sourcedata.fms
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class units extends fms {

  var transactionalTable: String = "transactional_unit"
  def setTransactionalTableName(tableName: String): Unit = this.transactionalTable = tableName
  var entityTable: String = "entity_unit"
  def setEntityTableName(tableName: String): Unit = this.entityTable = tableName
  var delimiter = "|"
  def setDelimiter(delimiter: String): Unit = this.delimiter = delimiter
  def setFileLocation(fileLocation: String): Unit = this.fileLocation = fileLocation

  def schema:StructType = {
    StructType(Array(
      StructField("UIC", StringType, nullable = false),
      StructField("UNIT_LNAME", StringType, nullable = true),
      StructField("UNIT_HOME_FORT_NAME", StringType, nullable = true),
      StructField("UNIT_LOCATION_ZIP_CODE", StringType, nullable = true),
      StructField("UNIT_GEOCODE", StringType, nullable = true),
      StructField("EquipUIC", StringType, nullable = true),
      StructField("UESSR_UIC_DESC", StringType, nullable = true),
      StructField("MODIFIED_UESSR_DESC", StringType, nullable = true),
      StructField("PRIMARY_UESSR_DESC", StringType, nullable = true),
      StructField("INTERMEDIATE_UESSR_DESC", StringType, nullable = true),
      StructField("SUPER_UESSR_DESC", StringType, nullable = true),
      StructField("ComponentCode", StringType, nullable = true)
    ))
  }

}
