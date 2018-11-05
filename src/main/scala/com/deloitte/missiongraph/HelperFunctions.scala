package com.deloitte.missiongraph

import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.datastax.driver.dse.DseSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._
import scala.collection.mutable

object HelperFunctions {

  def loadDataFromFile(spark: SparkSession, delimiter:String, schema: StructType, fileLocation: String): DataFrame = {
    val dataframeFromFile = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .schema(schema).load(fileLocation)
    return dataframeFromFile
  }

  def storeDataFrameToDSE(spark: SparkSession, dataFrametoPersist: DataFrame, table: String, keyspace: String, cluster_name: String): Unit = {
    dataFrametoPersist.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace, "cluster" -> cluster_name))
      .save()
  }

  def createKeyspace(session: DseSession, keyspace:String, datacenter: String, replication_factor: Int): Unit = {
    val replication:java.util.Map[String, Object] = mutable.Map("class" -> "NetworkTopologyStrategy", datacenter -> replication_factor.toString.asInstanceOf[AnyRef]).asJava
    val create_keyspace = SchemaBuilder.createKeyspace(keyspace).ifNotExists().`with`().replication(replication)
    session.execute(create_keyspace)
  }

  def mergeMap[T](map1: Map[T,List[T]], map2: Map[T,List[T]]) = {
    val mapSeq = map1.toList ++ map2.toList
    mapSeq.groupBy(_._1).map{case(k, v) => k -> v.flatMap(_._2) }
  }

  def mergeMapList[T](mapList: List[Map[T,List[T]]]) = {
    mapList.foldLeft(Map.empty[T,List[T]])((map, value) => mergeMap(map,value))
  }


}
