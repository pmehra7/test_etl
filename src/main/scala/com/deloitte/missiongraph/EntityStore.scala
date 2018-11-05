package com.deloitte.missiongraph

import org.apache.spark.sql.SparkSession

object EntityStore {

  // Get all records where processed != true
    // Confirm this works - you may have to insert processed = false
  // Flatten all records by UIC - take the most recent one

  val configData: com.typesafe.config.Config = config.configFile
  val clusterName: String = configData.getString("cluster")
  val keyspace: String = configData.getString("entity_keyspace")

  //soldier
  //events
  //units


  def writeToEntityStore = {
    null
  }


  def getUnprocessedTransactions(spark: SparkSession, keyspace: String, table: String) = {
    val transactions = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> table, "keyspace" -> keyspace)).load()
    transactions.filter("processed")
  }



  def getLastestPropetyValues = {
    null
  }



}
