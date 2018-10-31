package com.deloitte.missiongraph.domain

import com.deloitte.missiongraph.sourcedata.emilpo
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class events extends emilpo {

  var transactionalTable: String = "transactional_events"
  def setTransactionalTableName(tableName: String): Unit = this.transactionalTable = tableName
  var entityTable: String = "entity_events"
  def setEntityTableName(tableName: String): Unit = this.entityTable = tableName
  var delimiter = "|"
  def setDelimiter(delimiter: String): Unit = this.delimiter = delimiter
  def setFileLocation(fileLocation: String): Unit = this.fileLocation = fileLocation

  def schema:StructType = {
    StructType(Array(
      StructField("SSN", StringType, nullable = true),
      StructField("EVT_START_DT", StringType, nullable = true),
      StructField("UIC", StringType, nullable = false),
      StructField("DEPLOY_CNTRY_CD", StringType, nullable = true),
      StructField("ASSIGNED_UIC", StringType, nullable = true),
      StructField("IN_THEATER_CD", StringType, nullable = true),
      StructField("EVT_ACT_END_DT", StringType, nullable = true),
      StructField("EVT_CAT_CD", StringType, nullable = true),
      StructField("EVT_PURP_CD", StringType, nullable = true),
      StructField("deployed_UIC", StringType, nullable = true),
      StructField("eventCategory", StringType, nullable = true),
      StructField("daysDuration", StringType, nullable = true)
    ))
  }

}
