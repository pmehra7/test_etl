package com.deloitte.missiongraph

import com.datastax.driver.core.Row
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.datastax.driver.dse.{DseCluster, DseSession}
import com.deloitte.missiongraph.HelperFunctions.mergeMapList
import com.deloitte.missiongraph.domain.{events, soldier, units}
import scala.collection.JavaConverters._
import scala.collection.mutable

object test  {

  def main(args: Array[String]):Unit = {
    new ClusterConnection()
  }

  class ClusterConnection() {

    val configData: com.typesafe.config.Config = config.configFile

    val clusterIp: String = "127.0.0.1"
    val graphName = "test_graph_2"
    //val graphName = configData.getString("graph_name")

    val clusterBuilder: DseCluster = DseCluster.builder().addContactPoint(clusterIp).build()
    val session: DseSession = clusterBuilder.connect()

    val row: Row = session.execute("select release_version from system.local").one()
    System.out.println("Release Version: " + row.getString("release_version"))

    val transactional_keyspace = "test_transactional"
    val datacenter = "SearchGraphAnalytics"
    val replication_factor = 1


    createKeyspace(transactional_keyspace,datacenter,replication_factor)

    // Cassandra Schema
    val test = new events().dseSchemaTransactional(transactional_keyspace)
    val test1 = new events().dseSchemaEntity(transactional_keyspace)
    session.execute(test)
    session.execute(test1)

    val eventSchema = new events().getGraphSchema
    val soldierSchema = new soldier().getGraphSchema
    val unitsSchema = new units().getGraphSchema
    val graphSchemaStatements = mergeMapList(List(eventSchema,soldierSchema,unitsSchema))
    //executeGraphStatementTest(session, graphName, graphSchemaStatements)

    session.close()
    clusterBuilder.close()
    System.out.println("Cluster and Session closed")

    def createKeyspace(keyspace:String, datacenter: String, replication_factor: Int): Unit = {
      val replication:java.util.Map[String, Object] = mutable.Map("class" -> "NetworkTopologyStrategy", datacenter -> replication_factor.toString.asInstanceOf[AnyRef]).asJava
      val create_keyspace = SchemaBuilder.createKeyspace(keyspace).ifNotExists().`with`().replication(replication)
      session.execute(create_keyspace)
    }

  }

}