package com.deloitte.missiongraph

import java.io.File
import com.typesafe.config.ConfigFactory

object config {
  val configFile: com.typesafe.config.Config = ConfigFactory.parseFile(new File("config.conf"))
}
