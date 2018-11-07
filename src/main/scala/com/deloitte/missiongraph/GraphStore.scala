package com.deloitte.missiongraph


import com.datastax.bdp.graph.spark.graphframe.DseGraphFrame
import com.deloitte.missiongraph.GraphSchema.{Edge, Vertex, graphStruc}
import com.deloitte.missiongraph.HelperFunctions.storeDataFrameToDSE
import com.deloitte.missiongraph.TransactionalStore.units
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object GraphStore {

  val configData: com.typesafe.config.Config = config.configFile
  val clusterName: String = configData.getString("cluster")
  val keyspace_transactional: String = configData.getString("transactional_keyspace")
  val keyspace_entity: String = configData.getString("entity_keyspace")


  def writeUnitstoGraph(spark: SparkSession, graph: DseGraphFrame) = {
    val entities = getUnprocessedEntities(spark, keyspace_entity, units.entityTable).drop("processed").drop("marked_for_delete")
    writeToGraph(graph, entities, units.unitVertex)
    writeProcessedEntities(spark, entities, keyspace_entity, units.entityTable, clusterName)
  }


  def writeToGraph(graph: DseGraphFrame, data: DataFrame, graphStruct: graphStruc) = graphStruct match {
    case Edge(label, properties, cardinality, connection) =>{
      val propList = properties.map(x => x.name)
      val edge = data.select(propList.map(col): _*)
      val edgeData = edge.withColumn("srcLabel", lit(connection._1)).withColumn("dstLabel", lit(connection._2)).withColumn("edgeLabel", lit(label))
      graph.updateEdges(edgeData)
    }
    case Vertex(label, properties , primaryKey) => {
      val propList = (properties ::: primaryKey.valuesIterator.toList.flatten).map(x => x.name)
      val vertex = data.select(propList.map(col): _*)
      val vertexData = vertex.withColumn("~label", lit(label))
      graph.updateVertices(vertexData)
    }
  }

  private def getUnprocessedEntities(spark: SparkSession, keyspace: String, table: String) = {
    val entities = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> table, "keyspace" -> keyspace)).load()
    entities.filter("processed_graph is null")
  }

  private def writeProcessedEntities(spark: SparkSession, dfToWrite: DataFrame, table: String, keyspace: String, cluster_name: String) = {
    val entities = dfToWrite.select("uic").withColumn("processed_graph", lit(true))
    storeDataFrameToDSE(spark, entities, keyspace, table, cluster_name)
  }

}
