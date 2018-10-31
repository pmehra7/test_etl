package com.deloitte.missiongraph

import org.apache.spark.sql.SparkSession

object DataLoadBatch {
  def main(args: Array[String]):Unit = {


    val spark = SparkSession
      .builder
      .appName("Graph Load Application")
      .enableHiveSupport()
      .getOrCreate()

    TransactionalStore.soldierWriteToCassandra(spark)
    TransactionalStore.eventsWriteToCassandra(spark)
    TransactionalStore.unitsWriteToCassandra(spark)

  }
}
