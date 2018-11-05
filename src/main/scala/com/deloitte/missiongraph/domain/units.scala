package com.deloitte.missiongraph.domain

import com.datastax.driver.core.DataType
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.deloitte.missiongraph.GraphSchema._
import com.deloitte.missiongraph.sourcedata.fms
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class units extends fms {

  var transactionalTable: String = "transactional_unit"
  def setTransactionalTableName(tableName: String): Unit = this.transactionalTable = tableName
  var entityTable: String = "entity_unit"
  def setEntityTableName(tableName: String): Unit = this.entityTable = tableName
  var delimiter = "|"
  def setDelimiter(delimiter: String): Unit = this.delimiter = delimiter
  def setFileLocation(fileLocation: String): Unit = this.fileLocation = fileLocation
  def getSchemaColumns: List[(String,String)] = schema.toList.map(x => (x.productElement(0).toString, x.productElement(1).toString))

  def schema:StructType = {
    StructType(Array(
      StructField("UIC", StringType, nullable = false),
      StructField("UNIT_LNAME", StringType, nullable = true),
      StructField("UNIT_HOME_FORT_NAME", StringType, nullable = true),
      StructField("UNIT_LOCATION_ZIP_CODE", StringType, nullable = true),
      StructField("UNIT_GEOCODE", StringType, nullable = true),
      StructField("EquipUIC", StringType, nullable = true),
      StructField("UESSR_UIC_DESC", StringType, nullable = true),
      StructField("MODIFIED_UESSR_DESC", StringType, nullable = true),
      StructField("PRIMARY_UESSR_DESC", StringType, nullable = true),
      StructField("INTERMEDIATE_UESSR_DESC", StringType, nullable = true),
      StructField("SUPER_UESSR_DESC", StringType, nullable = true),
      StructField("ComponentCode", StringType, nullable = true)
    ))
  }

  def dseSchemaTransactional(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, transactionalTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addClusteringColumn("INSERT_TIME", DataType.timestamp())
      .addColumn("UNIT_LNAME", DataType.text())
      .addColumn("UNIT_HOME_FORT_NAME", DataType.text())
      .addColumn("UNIT_LOCATION_ZIP_CODE", DataType.text())
      .addColumn("UNIT_GEOCODE", DataType.text())
      .addColumn("EquipUIC", DataType.text())
      .addColumn("UESSR_UIC_DESC", DataType.text())
      .addColumn("MODIFIED_UESSR_DESC", DataType.text())
      .addColumn("PRIMARY_UESSR_DESC", DataType.text())
      .addColumn("INTERMEDIATE_UESSR_DESC", DataType.text())
      .addColumn("SUPER_UESSR_DESC", DataType.text())
      .addColumn("ComponentCode", DataType.text())
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
      .addColumn("UNIT_LNAME", DataType.text())
      .addColumn("UNIT_HOME_FORT_NAME", DataType.text())
      .addColumn("UNIT_LOCATION_ZIP_CODE", DataType.text())
      .addColumn("UNIT_GEOCODE", DataType.text())
      .addColumn("EquipUIC", DataType.text())
      .addColumn("UESSR_UIC_DESC", DataType.text())
      .addColumn("MODIFIED_UESSR_DESC", DataType.text())
      .addColumn("PRIMARY_UESSR_DESC", DataType.text())
      .addColumn("INTERMEDIATE_UESSR_DESC", DataType.text())
      .addColumn("SUPER_UESSR_DESC", DataType.text())
      .addColumn("ComponentCode", DataType.text())
      //.addColumn("processed", DataType.cboolean())
      //.addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  private val partitionProperties = List(
    property("UIC",GraphDataType.Text,Cardinality.single)
  )

  private val clusteringProperties = List(
    property("INSERT_TIME",GraphDataType.Text,Cardinality.single)
  )

  private val properties = List(
    property("UNIT_LNAME",GraphDataType.Text,Cardinality.single),
    property("UNIT_HOME_FORT_NAME",GraphDataType.Text,Cardinality.single),
    property("UNIT_LOCATION_ZIP_CODE",GraphDataType.Text,Cardinality.single),
    property("UNIT_GEOCODE",GraphDataType.Text,Cardinality.single),
    property("EquipUIC",GraphDataType.Text,Cardinality.single)
  )

  private val allProperties = partitionProperties ::: clusteringProperties ::: properties

  private val unitVertex = vertex("unit", properties, partitionProperties, clusteringProperties)

  def getGraphSchema: Map[String, List[String]] = Map(
    GraphObject.Vertex.toString -> List(createVertexSchema(unitVertex)),
    GraphObject.Property.toString -> createPropertySchema(allProperties)
  )

}
