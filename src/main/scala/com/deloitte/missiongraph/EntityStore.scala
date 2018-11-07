package com.deloitte.missiongraph


import com.deloitte.missiongraph.HelperFunctions._
import com.deloitte.missiongraph.TransactionalStore._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}


object EntityStore {

  val configData: com.typesafe.config.Config = config.configFile
  val clusterName: String = configData.getString("cluster")
  val keyspace_transactional: String = configData.getString("transactional_keyspace")
  val keyspace_entity: String = configData.getString("entity_keyspace")


  def writeUnitstoEntityStore(spark: SparkSession) = {
    val entities = getUnprocessedTransactions(spark, keyspace_transactional, units.transactionalTable).drop("processed").drop("marked_for_delete")
    writeToEntityStore(spark, entities.drop("insert_time"), keyspace_entity, units.entityTable, clusterName)
    val transactionUpdates = entities
    writeProcessedTransactions(spark, transactionUpdates, keyspace_transactional, units.transactionalTable, clusterName)
  }

  private def writeToEntityStore(spark: SparkSession, dfToWrite: DataFrame, table: String, keyspace: String, cluster_name: String) = {
    storeDataFrameToDSE(spark, dfToWrite, keyspace, table, cluster_name)
  }

  private def writeProcessedTransactions(spark: SparkSession, dfToWrite: DataFrame, table: String, keyspace: String, cluster_name: String) = {
    val transactions = dfToWrite.select("uic","insert_time").withColumn("processed", lit(true))
    storeDataFrameToDSE(spark, transactions, keyspace, table, cluster_name)
  }


  private def getUnprocessedTransactions(spark: SparkSession, keyspace: String, table: String) = {
    val transactions = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> table, "keyspace" -> keyspace)).load()
    transactions.filter("processed is null")
  }

  // Flatten all records by UIC - take the most recent one - Talk this through during demo - this is where you would control atomicity
  def getLastestPropertyValues = {
    null
  }



}
