package com.deloitte.missiongraph.domain

import com.datastax.driver.core.DataType
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.deloitte.missiongraph.GraphSchema._
import com.deloitte.missiongraph.sourcedata.emilpo
import org.apache.spark.sql.types._

class events extends emilpo {

  var transactionalTable: String = "transactional_events"
  def setTransactionalTableName(tableName: String): Unit = this.transactionalTable = tableName
  var entityTable: String = "entity_events"
  def setEntityTableName(tableName: String): Unit = this.entityTable = tableName
  var delimiter = "|"
  def setDelimiter(delimiter: String): Unit = this.delimiter = delimiter
  def setFileLocation(fileLocation: String): Unit = this.fileLocation = fileLocation
  def getSchemaColumns: List[(String,String)] = schema.toList.map(x => (x.productElement(0).toString,x.productElement(1).toString))

  // Spark Schema for Data Source Read
  def schema:StructType = {
    StructType(Array(
      StructField("SSN", StringType, nullable = true),
      StructField("EVT_START_DT", StringType, nullable = true),
      StructField("UIC", StringType, nullable = false),
      StructField("DEPLOY_CNTRY_CD", StringType, nullable = true),
      StructField("ASSIGNED_UIC", StringType, nullable = true),
      StructField("IN_THEATER_CD", StringType, nullable = true),
      StructField("EVT_ACT_END_DT", StringType, nullable = true),
      StructField("EVT_CAT_CD", StringType, nullable = true),
      StructField("EVT_PURP_CD", StringType, nullable = true),
      StructField("deployed_UIC", StringType, nullable = true),
      StructField("eventCategory", StringType, nullable = true),
      StructField("daysDuration", StringType, nullable = true)
    ))
  }

  def dseSchemaTransactional(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, transactionalTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addClusteringColumn("INSERT_TIME", DataType.timestamp())
      .addColumn("SSN", DataType.text())
      .addColumn("EVT_START_DT", DataType.text())
      .addColumn("DEPLOY_CNTRY_CD", DataType.text())
      .addColumn("ASSIGNED_UIC", DataType.text())
      .addColumn("IN_THEATER_CD", DataType.text())
      .addColumn("EVT_ACT_END_DT", DataType.text())
      .addColumn("EVT_CAT_CD", DataType.text())
      .addColumn("EVT_PURP_CD", DataType.text())
      .addColumn("deployed_UIC", DataType.text())
      .addColumn("deployed_UIC", DataType.text())
      .addColumn("processed", DataType.cboolean())
      .addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }


  def dseSchemaEntity(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, entityTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addColumn("SSN", DataType.text())
      .addColumn("EVT_START_DT", DataType.text())
      .addColumn("DEPLOY_CNTRY_CD", DataType.text())
      .addColumn("ASSIGNED_UIC", DataType.text())
      .addColumn("IN_THEATER_CD", DataType.text())
      .addColumn("EVT_ACT_END_DT", DataType.text())
      .addColumn("EVT_CAT_CD", DataType.text())
      .addColumn("EVT_PURP_CD", DataType.text())
      .addColumn("deployed_UIC", DataType.text())
      .addColumn("deployed_UIC", DataType.text())
      //.addColumn("processed", DataType.cboolean())
      //.addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  // Graph Schema
  val properties = List(
    property("EVT_START_DT",GraphDataType.Text,Cardinality.single),
    property("SSN",GraphDataType.Text,Cardinality.single),
    property("DEPLOY_CNTRY_CD",GraphDataType.Text,Cardinality.single),
    property("UNIT_GEOCODE",GraphDataType.Text,Cardinality.single),
    property("eventCategory",GraphDataType.Text,Cardinality.single),
    property("daysDuration",GraphDataType.Text,Cardinality.single)
  )

  private val allProperties = properties

  val eventEdge = edge("resides_in", properties, Cardinality.multiple, ("soldier","unit"))

  def getGraphSchema: Map[String, List[String]] = {
    Map(
      GraphObject.Edge.toString -> List(createEdgeSchema(eventEdge)),
      GraphObject.Property.toString -> createPropertySchema(allProperties)
    )
  }


}
