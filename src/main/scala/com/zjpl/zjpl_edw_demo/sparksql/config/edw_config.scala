package com.zjpl.zjpl_edw_demo.sparksql.config

import java.io.{BufferedInputStream, InputStream}
import java.util.Properties

object edw_config {
 val properties:Properties = new Properties()
  val in:InputStream = getClass.getResourceAsStream("/config/zjpl_config.properties")
  properties.load(new BufferedInputStream(in))
}
