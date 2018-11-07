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
  def getGraphPropertyCols = allProperties.map(x => x.name)


  // Spark Schema for Data Source Read
  def schema:StructType = {
    StructType(Array(
      StructField("ssn", StringType, nullable = true),
      StructField("evt_start_dt", StringType, nullable = true),
      StructField("uic", StringType, nullable = false),
      StructField("deploy_cntry_cd", StringType, nullable = true),
      StructField("assigned_uic", StringType, nullable = true),
      StructField("in_theater_cd", StringType, nullable = true),
      StructField("evt_act_end_dt", StringType, nullable = true),
      StructField("evt_cat_cd", StringType, nullable = true),
      StructField("evt_purp_cd", StringType, nullable = true),
      StructField("deployed_uic", StringType, nullable = true),
      StructField("eventcategory", StringType, nullable = true),
      StructField("daysduration", StringType, nullable = true)
    ))
  }

  def dseSchemaTransactional(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, transactionalTable)
      .ifNotExists()
      .addPartitionKey("uic", DataType.text())
      .addClusteringColumn("insert_time", DataType.timestamp())
      .addColumn("ssn", DataType.text())
      .addColumn("evt_start_dt", DataType.text())
      .addColumn("deploy_cntry_cd", DataType.text())
      .addColumn("assigned_uic", DataType.text())
      .addColumn("in_theater_cd", DataType.text())
      .addColumn("evt_act_end_dt", DataType.text())
      .addColumn("evt_cat_cd", DataType.text())
      .addColumn("evt_purp_cd", DataType.text())
      .addColumn("deployed_uic", DataType.text())
      .addColumn("eventcategory", DataType.text())
      .addColumn("daysduration", DataType.text())
      .addColumn("processed", DataType.cboolean())
      .addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }


  def dseSchemaEntity(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, entityTable)
      .ifNotExists()
      .addPartitionKey("uic", DataType.text())
      //.addClusteringColumn("insert_time", DataType.timestamp())
      .addColumn("ssn", DataType.text())
      .addColumn("evt_start_dt", DataType.text())
      .addColumn("deploy_cntry_cd", DataType.text())
      .addColumn("assigned_uic", DataType.text())
      .addColumn("in_theater_cd", DataType.text())
      .addColumn("evt_act_end_dt", DataType.text())
      .addColumn("evt_cat_cd", DataType.text())
      .addColumn("evt_purp_cd", DataType.text())
      .addColumn("deployed_uic", DataType.text())
      .addColumn("eventcategory", DataType.text())
      .addColumn("daysduration", DataType.text())
      .addColumn("processed_graph", DataType.cboolean())
      //.addColumn("processed", DataType.cboolean())
      //.addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  // Graph Schema
  val properties = List(
    Property("evt_start_dt",GraphDataType.Text,Cardinality.single),
    Property("ssn",GraphDataType.Text,Cardinality.single),
    Property("deploy_cntry_cd",GraphDataType.Text,Cardinality.single)
  )

  private val allProperties = properties

  val eventEdge = Edge("resides_in", properties, Cardinality.multiple, ("soldier","unit"))

  def getGraphSchema: Map[String, List[String]] = {
    Map(
      GraphObject.Edge.toString -> List(createEdgeSchema(eventEdge)),
      GraphObject.Property.toString -> createPropertySchema(allProperties)
    )
  }


}
