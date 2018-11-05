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

  case class property(name: String, dataType: GraphDataType.Value, cardinality: Cardinality.Value)

  case class vertex(label: String, properties: List[property], partitionKey: List[property], clusteringKey: List[property])

  case class edge(label: String, properties: List[property], cardinality: Cardinality.Value, connection: (String,String))
  //case class edge(label: String, properties: List[property], cardinality: Cardinality.Value, connection: (vertex,vertex))


  def createPropertySchema(allProperties: List[property]): List[String] = {
    val listOfCreateProps = allProperties.map(x => s"""schema.propertyKey(\"${x.name}\").${x.dataType}().ifNotExists().create()""")
    listOfCreateProps
  }

  def createVertexSchema(vertex: vertex): String = {
    val partitionKeyString = vertex.partitionKey.map(x => x.name).mkString("\"", "\",\"", "\"")
    val clusteringKeyString = vertex.clusteringKey.map(x => x.name).mkString("\"", "\",\"", "\"")
    val propertyStrings = vertex.properties.map(x => x.name).mkString("\"", "\",\"", "\"")
    val schemaString = s"""schema.vertexLabel(\"${vertex.label}\")"""
    val createPartitionKey = s""".partitionKey($partitionKeyString)"""
    val createClusterKey = s""".clusteringKey($clusteringKeyString)"""
    val createProperties = s""".properties($propertyStrings)"""
    val createVertexString = schemaString + createPartitionKey + createClusterKey + createProperties + ".ifNotExists().create()"
    createVertexString
  }

  def createEdgeSchema(edge: edge): String = {
    val propertyStrings = edge.properties.map(x => x.name).mkString("\"", "\",\"", "\"")
    val connectionStrings = s"""\"${edge.connection._1}\", \"${edge.connection._2}\""""
    val schemaString = s"""schema.edgeLabel(\"${edge.label}\").${edge.cardinality.toString}()"""
    val createProperties = s""".properties($propertyStrings)"""
    val createConnection = s""".connection($connectionStrings)"""
    val createEdgeString = schemaString + createProperties + createConnection + ".ifNotExists().create()"
    createEdgeString
  }

  def createGraph(dseSession: DseSession, graphName: String, mapOfSchemaStatements: Map[String, List[String]]) = {
    dseSession.executeGraph(s"system.graph('${graphName}').ifNotExists().create()")
    try {
      mapOfSchemaStatements(GraphObject.Property.toString).foreach(x => dseSession.executeGraph(new SimpleGraphStatement(x).setGraphName(graphName)))
      mapOfSchemaStatements(GraphObject.Vertex.toString).foreach(x => dseSession.executeGraph(new SimpleGraphStatement(x).setGraphName(graphName)))
      mapOfSchemaStatements(GraphObject.Edge.toString).foreach(x => dseSession.executeGraph(new SimpleGraphStatement(x).setGraphName(graphName)))
    } catch {
      case e: java.util.NoSuchElementException => logger.debug("INCORRECT GRAPH SCHEMA")
      //case e1: => logger.debug("")
    }
  }

  def executeGraphStatementTest(dseSession: DseSession, graphName: String, mapOfSchemaStatements: Map[String, List[String]]) = {
    dseSession.executeGraph(s"system.graph('${graphName}').ifNotExists().create()")
    try {
      mapOfSchemaStatements(GraphObject.Property.toString).foreach(x => println(x))
      mapOfSchemaStatements(GraphObject.Vertex.toString).foreach(x => println(x))
      mapOfSchemaStatements(GraphObject.Edge.toString).foreach(x => println(x))
    } catch {
      case e: java.util.NoSuchElementException => logger.debug("INCORRECT GRAPH SCHEMA")
      //case e1: => logger.debug("")
    }
  }




}
