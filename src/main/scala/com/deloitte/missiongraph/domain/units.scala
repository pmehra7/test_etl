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
  def getGraphPropertyCols = allProperties.map(x => x.name)


  def schema:StructType = {
    StructType(Array(
      StructField("uic", StringType, nullable = false),
      StructField("unit_lname", StringType, nullable = true),
      StructField("unit_home_fort_name", StringType, nullable = true),
      StructField("unit_location_zip_code", StringType, nullable = true),
      StructField("unit_geocode", StringType, nullable = true),
      StructField("equipuic", StringType, nullable = true),
      StructField("uessr_uic_desc", StringType, nullable = true),
      StructField("modified_uessr_desc", StringType, nullable = true),
      StructField("primary_uessr_desc", StringType, nullable = true),
      StructField("intermediate_uessr_desc", StringType, nullable = true),
      StructField("super_uessr_desc", StringType, nullable = true),
      StructField("componentcode", StringType, nullable = true)
    ))
  }

  def dseSchemaTransactional(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, transactionalTable)
      .ifNotExists()
      .addPartitionKey("uic", DataType.text())
      .addClusteringColumn("insert_time", DataType.timestamp())
      .addColumn("unit_lname", DataType.text())
      .addColumn("unit_home_fort_name", DataType.text())
      .addColumn("unit_location_zip_code", DataType.text())
      .addColumn("unit_geocode", DataType.text())
      .addColumn("equipuic", DataType.text())
      .addColumn("uessr_uic_desc", DataType.text())
      .addColumn("modified_uessr_desc", DataType.text())
      .addColumn("primary_uessr_desc", DataType.text())
      .addColumn("intermediate_uessr_desc", DataType.text())
      .addColumn("super_uessr_desc", DataType.text())
      .addColumn("componentcode", DataType.text())
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
      .addColumn("unit_lname", DataType.text())
      .addColumn("unit_home_fort_name", DataType.text())
      .addColumn("unit_location_zip_code", DataType.text())
      .addColumn("unit_geocode", DataType.text())
      .addColumn("equipuic", DataType.text())
      .addColumn("uessr_uic_desc", DataType.text())
      .addColumn("modified_uessr_desc", DataType.text())
      .addColumn("primary_uessr_desc", DataType.text())
      .addColumn("intermediate_uessr_desc", DataType.text())
      .addColumn("super_uessr_desc", DataType.text())
      .addColumn("componentcode", DataType.text())
      .addColumn("processed_graph", DataType.cboolean())
      //.addColumn("processed", DataType.cboolean())
      //.addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }


  private val primaryKey = Map(
    PrimaryKey.PartitionKey -> List(Property("uic",GraphDataType.Text,Cardinality.single))
  )

  private val properties = List(
    Property("unit_lname",GraphDataType.Text,Cardinality.single),
    Property("unit_home_fort_name",GraphDataType.Text,Cardinality.single),
    Property("unit_location_zip_code",GraphDataType.Text,Cardinality.single),
    Property("unit_geocode",GraphDataType.Text,Cardinality.single),
    Property("equipuic",GraphDataType.Text,Cardinality.single)
  )

  private val allProperties = primaryKey.valuesIterator.toList.flatten ::: properties

  val unitVertex = Vertex("unit", properties, primaryKey)

  def getGraphSchema: Map[String, List[String]] = Map(
    GraphObject.Vertex.toString -> List(createVertexSchema(unitVertex)),
    GraphObject.Property.toString -> createPropertySchema(allProperties)
  )

}
