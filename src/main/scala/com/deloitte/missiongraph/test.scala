package com.deloitte.missiongraph

import com.deloitte.missiongraph.GraphSchema._

object test  {

  def main(args: Array[String]):Unit = {
    new ClusterConnection()
  }

  class ClusterConnection() {

    val clusterIp: String = "127.0.0.1"
    val graphName = "test_graph_2"
    val transactional_keyspace = "test_transactional"
    val datacenter = "SearchGraphAnalytics"
    val replication_factor = 1

    //val clusterBuilder: DseCluster = DseCluster.builder().addContactPoint(clusterIp).build()
    //val session: DseSession = clusterBuilder.connect()

    //val row: Row = session.execute("select release_version from system.local").one()
    //System.out.println("Release Version: " + row.getString("release_version"))

    val prop = List(Property("tinker", GraphSchema.GraphDataType.Text, GraphSchema.Cardinality.single))
    val prim = Map(
      GraphSchema.PrimaryKey.PartitionKey -> List(Property("test_part", GraphSchema.GraphDataType.Text, GraphSchema.Cardinality.single)),
      GraphSchema.PrimaryKey.ClusteringKey -> List(Property("test_cluster", GraphSchema.GraphDataType.Text, GraphSchema.Cardinality.single))
    )

    val nullprop = Option(List(Property))

    abstract class graphStruc

    case class Property(name: String, dataType: GraphDataType.Value, cardinality: Cardinality.Value)

    case class Vertex(label: String, properties: List[Property], primaryKey: Map[PrimaryKey.Value, List[Property]]) extends graphStruc

    case class Edge(label: String, properties: List[Property], cardinality: Cardinality.Value, connection: (String,String)) extends graphStruc

    val x = Vertex("Test_Vertex",prop,prim)
    val y = Edge("Test_Edge",prop, GraphSchema.Cardinality.single,("Test1","Test2"))


    def createVertexSchema(vertex: Vertex): Unit = {
      val partitionKeyString = if (vertex.primaryKey.contains(PrimaryKey.PartitionKey)) vertex.primaryKey(PrimaryKey.PartitionKey).map(x => x.name).mkString("\"", "\",\"", "\"")
      val clusteringKeyString = if (vertex.primaryKey.contains(PrimaryKey.ClusteringKey)) vertex.primaryKey(PrimaryKey.ClusteringKey).map(x => x.name).mkString("\"", "\",\"", "\"")
      val propertyStrings = vertex.properties.map(x => x.name).mkString("\"", "\",\"", "\"")
      val schemaString = s"""schema.vertexLabel(\"${vertex.label}\")"""
      val createPartitionKey = if (vertex.primaryKey.contains(PrimaryKey.PartitionKey)) s""".partitionKey($partitionKeyString)""" else ""
      val createClusterKey = if (vertex.primaryKey.contains(PrimaryKey.ClusteringKey)) s""".clusteringKey($clusteringKeyString)""" else ""
      val createProperties = s""".properties($propertyStrings)"""
      val createVertexString = schemaString + createPartitionKey + createClusterKey + createProperties + ".ifNotExists().create()"
      println(createVertexString)
    }


    //writeToGraph(y)
    //writeToGraph(x)

    createVertexSchema(x)

    // graph: DseGraphFrame, data: DataFrame,
    def writeToGraph(graphStruct: graphStruc) = graphStruct match {
      case Edge(label, properties, cardinality, connection) => {
        println(s"edge ${label}")
        //graph.updateEdges(data)
      }
      case Vertex(label, properties , primaryKey) => {
        println(s"vertex ${label}")
        //graph.updateVertices(data)
      }
    }


//    createKeyspace(transactional_keyspace,datacenter,replication_factor)
//
//    // Cassandra Schema
//    val test = new events().dseSchemaTransactional(transactional_keyspace)
//    val test1 = new events().dseSchemaEntity(transactional_keyspace)
//    session.execute(test)
//    session.execute(test1)
//
//    val eventSchema = new events().getGraphSchema
//    val soldierSchema = new soldier().getGraphSchema
//    val unitsSchema = new units().getGraphSchema
//    val graphSchemaStatements = mergeMapList(List(eventSchema,soldierSchema,unitsSchema))
//    //executeGraphStatementTest(session, graphName, graphSchemaStatements)

    //session.close()
    //clusterBuilder.close()
    //System.out.println("Cluster and Session closed")

//    def createKeyspace(keyspace:String, datacenter: String, replication_factor: Int): Unit = {
//      val replication:java.util.Map[String, Object] = mutable.Map("class" -> "NetworkTopologyStrategy", datacenter -> replication_factor.toString.asInstanceOf[AnyRef]).asJava
//      val create_keyspace = SchemaBuilder.createKeyspace(keyspace).ifNotExists().`with`().replication(replication)
//      session.execute(create_keyspace)
//    }

  }

}