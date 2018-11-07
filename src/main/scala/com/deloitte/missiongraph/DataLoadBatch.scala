package com.deloitte.missiongraph

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.SparkSession

object DataLoadBatch {
  def main(args: Array[String]):Unit = {

    val spark = SparkSession
      .builder
      .appName("MissionGraph Load App")
      .config("spark.cassandra.output.ignoreNulls", "true")
      .enableHiveSupport()
      .getOrCreate

    val configData: com.typesafe.config.Config = config.configFile
    val graphName: String = configData.getString("graph_name")

    val graph = spark.dseGraph(graphName)

    //TransactionalStore.soldierWriteToCassandra(spark)
    //TransactionalStore.eventsWriteToCassandra(spark)
    TransactionalStore.unitsWriteToCassandra(spark)

    EntityStore.writeUnitstoEntityStore(spark)

    GraphStore.writeUnitstoGraph(spark, graph)

  }
}
