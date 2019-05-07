package com.zjpl.zjpl_edw_demo.sparksql.Dao

/**
  *
  * @param id
  * @param durationMs
  * @param numEvents
  * @param expired
  */
case class SessionUpdate(id: String,
                         durationMs: Long,
                         numEvents: Int,
                         expired: Boolean)

//scalastyle:on prinln