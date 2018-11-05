package com.deloitte.missiongraph

import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import com.datastax.driver.dse.{DseCluster, DseSession}
import com.deloitte.missiongraph.GraphSchema.createGraph
import com.deloitte.missiongraph.HelperFunctions.mergeMapList
import com.deloitte.missiongraph.domain._

import scala.collection.JavaConverters._
import scala.collection.mutable

object CreateSchema  {

  def main(args: Array[String]):Unit = {
    new ClusterConnection()
  }

  class ClusterConnection() {

    // Get cluster configurations
    val configData: com.typesafe.config.Config = config.configFile
    val clusterIp: String = configData.getString("cluster_ip")
    val datacenter: String = configData.getString("datacenter")
    val replication_factor: Int = configData.getInt("replication_factor")
    val graphName: String = configData.getString("graph_name")
    val username: String = configData.getString("username")
    val password: String = configData.getString("password")
    val transactional_keyspace: String = configData.getString("transactional_keyspace")
    val entity_keyspace: String = configData.getString("entity_keyspace")

    // Create domain objects to get schemas
    val event  = new events()
    val soldier = new soldier()
    val unit = new units()

    // Connect to DSE
    val clusterBuilder: DseCluster = DseCluster.builder().addContactPoint(clusterIp).build() //.withCredentials(username, password)
    val session: DseSession = clusterBuilder.connect()

    // Create Keyspaces
    System.out.println("Creating Transactional Keyspace")
    createKeyspace(transactional_keyspace,datacenter,replication_factor)

    System.out.println("Creating Entity Keyspace")
    createKeyspace(entity_keyspace,datacenter,replication_factor)

    // Create Tables
    createTable(event.dseSchemaTransactional(transactional_keyspace))
    createTable(event.dseSchemaEntity(entity_keyspace))

    createTable(soldier.dseSchemaTransactional(transactional_keyspace))
    createTable(soldier.dseSchemaEntity(entity_keyspace))

    createTable(unit.dseSchemaTransactional(transactional_keyspace))
    createTable(unit.dseSchemaEntity(entity_keyspace))

    // Create Graphs
    System.out.println("Creating Graph Schema")
    val graphSchemaStatements = mergeMapList(List(event.getGraphSchema,soldier.getGraphSchema,unit.getGraphSchema))
    createGraph(session, graphName, graphSchemaStatements)

    // Close cluster and session
    session.close()
    clusterBuilder.close()
    System.out.println("Cluster and Session closed")


    def createKeyspace(keyspace:String, datacenter: String, replication_factor: Int): Unit = {
      val replication:java.util.Map[String, Object] = mutable.Map("class" -> "NetworkTopologyStrategy", datacenter -> replication_factor.toString.asInstanceOf[AnyRef]).asJava
      val create_keyspace = SchemaBuilder.createKeyspace(keyspace).ifNotExists().`with`().replication(replication)
      session.execute(create_keyspace)
    }

    def createTable(tableSchema: Create.Options): Unit = {
      session.execute(tableSchema)
    }

  }

}
