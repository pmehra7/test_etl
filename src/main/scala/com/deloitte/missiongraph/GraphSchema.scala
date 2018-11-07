package com.deloitte.missiongraph

import com.datastax.driver.dse.DseSession
import com.datastax.driver.dse.graph.SimpleGraphStatement
import com.typesafe.scalalogging.LazyLogging


object GraphSchema extends LazyLogging  {

  object Cardinality extends Enumeration {
    val single, multiple = Value
  }

  object GraphDataType extends Enumeration {
    val Text, Int, Bigint, Boolean, Float, Timestamp, Uuid = Value
  }

  object GraphObject extends Enumeration {
    val Edge, Vertex, Property = Value
  }

  object PrimaryKey extends Enumeration {
    val PartitionKey, ClusteringKey = Value
  }

  abstract class graphStruc

  case class Property(name: String, dataType: GraphDataType.Value, cardinality: Cardinality.Value)

  case class Vertex(label: String, properties: List[Property], primaryKey: Map[PrimaryKey.Value, List[Property]]) extends graphStruc

  case class Edge(label: String, properties: List[Property], cardinality: Cardinality.Value, connection: (String,String)) extends graphStruc

  def createPropertySchema(allProperties: List[Property]): List[String] = {
    val listOfCreateProps = allProperties.map(x => s"""schema.propertyKey(\"${x.name}\").${x.dataType}().ifNotExists().create()""")
    listOfCreateProps
  }


  def createVertexSchema(vertex: Vertex): String = {
    //val partitionKeyString = vertex.partitionKey.map(x => x.name).mkString("\"", "\",\"", "\"")
    //val clusteringKeyString = vertex.clusteringKey.map(x => x.name).mkString("\"", "\",\"", "\"")
    val partitionKeyString = if (vertex.primaryKey.contains(PrimaryKey.PartitionKey)) vertex.primaryKey(PrimaryKey.PartitionKey).map(x => x.name).mkString("\"", "\",\"", "\"")
    val clusteringKeyString = if (vertex.primaryKey.contains(PrimaryKey.ClusteringKey)) vertex.primaryKey(PrimaryKey.ClusteringKey).map(x => x.name).mkString("\"", "\",\"", "\"")
    val propertyStrings = vertex.properties.map(x => x.name).mkString("\"", "\",\"", "\"")
    val schemaString = s"""schema.vertexLabel(\"${vertex.label}\")"""
    val createPartitionKey = if (vertex.primaryKey.contains(PrimaryKey.PartitionKey)) s""".partitionKey($partitionKeyString)""" else ""
    val createClusterKey = if (vertex.primaryKey.contains(PrimaryKey.ClusteringKey)) s""".clusteringKey($clusteringKeyString)""" else ""
    val createProperties = s""".properties($propertyStrings)"""
    val createVertexString = schemaString + createPartitionKey + createClusterKey + createProperties + ".ifNotExists().create()"
    println(createVertexString)
    createVertexString
  }


  def createEdgeSchema(edge: Edge): String = {
    val propertyStrings = edge.properties.map(x => x.name).mkString("\"", "\",\"", "\"")
    val connectionStrings = s"""\"${edge.connection._1}\", \"${edge.connection._2}\""""
    val schemaString = s"""schema.edgeLabel(\"${edge.label}\").${edge.cardinality.toString}()"""
    val createProperties = s""".properties($propertyStrings)"""
    val createConnection = s""".connection($connectionStrings)"""
    val createEdgeString = schemaString + createProperties + createConnection + ".ifNotExists().create()"
    println(createEdgeString)
    createEdgeString
  }

  def createGraph(dseSession: DseSession, graphName: String, mapOfSchemaStatements: Map[String, List[String]]) = {
    dseSession.executeGraph(s"system.graph('${graphName}').ifNotExists().create()")
    try {
      mapOfSchemaStatements(GraphObject.Property.toString).foreach(x => dseSession.executeGraph(new SimpleGraphStatement(x).setGraphName(graphName)))
      mapOfSchemaStatements(GraphObject.Vertex.toString).foreach(x => dseSession.executeGraph(new SimpleGraphStatement(x).setGraphName(graphName)))
      mapOfSchemaStatements(GraphObject.Edge.toString).foreach(x => dseSession.executeGraph(new SimpleGraphStatement(x).setGraphName(graphName)))
    } catch {
      case e: java.util.NoSuchElementException => logger.debug("INCORRECT GRAPH SCHEMA- CHECK SCHEMA")
      //case e1: com.datastax.driver.core.exceptions.InvalidQueryException => logger.debug("ISSUE WITH EXECUTING QUERY")
    }
  }


}
