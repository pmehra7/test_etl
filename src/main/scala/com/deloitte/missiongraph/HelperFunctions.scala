package com.deloitte.missiongraph

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object HelperFunctions {

  def loadDataFromFile(spark: SparkSession, delimiter:String, schema: StructType, fileLocation: String): DataFrame = {
    val dataframeFromFile = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .schema(schema).load(fileLocation)
    return dataframeFromFile
  }

  def storeDataFrameToDSE(spark: SparkSession, dataFrametoPersist: DataFrame, table: String, keyspace: String, cluster_name: String): Unit = {
    dataFrametoPersist.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace, "cluster" -> cluster_name))
      .save()
  }

}
