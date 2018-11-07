package com.deloitte.missiongraph


import com.deloitte.missiongraph.HelperFunctions.{loadDataFromFile, storeDataFrameToDSE}
import com.deloitte.missiongraph.domain._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp


object TransactionalStore {

  val configData: com.typesafe.config.Config = config.configFile
  val clusterName: String = configData.getString("cluster")
  val keyspace: String = configData.getString("transactional_keyspace")

  val soldier = new soldier()
  soldier.setFileLocation(configData.getString("emilpo_soldier_file_location"))

  def soldierWriteToCassandra(spark: SparkSession) = {
    val soldierData = loadDataFromFile(spark, soldier.delimiter, soldier.schema, soldier.fileLocation).withColumn("insert_time", current_timestamp())
    storeDataFrameToDSE(spark, soldierData, soldier.transactionalTable, keyspace, clusterName)
  }

  val events = new events()
  events.setFileLocation(configData.getString("events_file_location"))

  def eventsWriteToCassandra(spark: SparkSession) = {
    val eventsData = loadDataFromFile(spark, events.delimiter, events.schema, events.fileLocation).withColumn("insert_time", current_timestamp())
    storeDataFrameToDSE(spark, eventsData, events.transactionalTable, keyspace, clusterName)
  }

  val units = new units()
  units.setFileLocation(configData.getString("units_file_location"))

  def unitsWriteToCassandra(spark: SparkSession) = {
    val unitsData = loadDataFromFile(spark, units.delimiter, units.schema, units.fileLocation).withColumn("insert_time", current_timestamp())
    storeDataFrameToDSE(spark, unitsData, units.transactionalTable, keyspace, clusterName)
  }

}
