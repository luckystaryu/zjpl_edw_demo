package com.zjpl.zjpl_edw_demo.sparksql.Dao

import java.sql.Timestamp

/**
  * User-defined data type representing the input events
  *
  * @param sessionId
  * @param timestamp
  */
case class Event(sessionId: String, timestamp: Timestamp)
