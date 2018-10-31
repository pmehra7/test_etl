package com.deloitte.missiongraph

import com.deloitte.missiongraph.HelperFunctions.{loadDataFromFile, storeDataFrameToDSE}
import com.deloitte.missiongraph.domain._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp


object TransactionalStore {

  val configData: com.typesafe.config.Config = config.configFile
  val clusterName: String = configData.getString("cluster")
  val keyspace: String = configData.getString("keyspace")

  val soldier_emilpo = new soldier()
  soldier_emilpo.setFileLocation(configData.getString("emilpo_soldier_file_location"))

  def soldierWriteToCassandra(spark: SparkSession) = {
    val soldierData = loadDataFromFile(spark, soldier_emilpo.delimiter, soldier_emilpo.schema, soldier_emilpo.fileLocation).withColumn("INSERT_TIME", current_timestamp())
    storeDataFrameToDSE(spark, soldierData, soldier_emilpo.transactionalTable, keyspace, clusterName)
  }

  val events = new events()
  events.setFileLocation(configData.getString("events_file_location"))

  def eventsWriteToCassandra(spark: SparkSession) = {
    val eventsData = loadDataFromFile(spark, events.delimiter, events.schema, events.fileLocation).withColumn("INSERT_TIME", current_timestamp())
    storeDataFrameToDSE(spark, eventsData, soldier_emilpo.transactionalTable, keyspace, clusterName)
  }

  val units = new units()
  units.setFileLocation(configData.getString("units_file_location"))

  def unitsWriteToCassandra(spark: SparkSession) = {
    val unitsData = loadDataFromFile(spark, units.delimiter, units.schema, units.fileLocation).withColumn("INSERT_TIME", current_timestamp())
    storeDataFrameToDSE(spark, unitsData, units.transactionalTable, keyspace, clusterName)
  }

}
