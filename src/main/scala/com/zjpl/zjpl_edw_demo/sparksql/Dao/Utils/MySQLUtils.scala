package com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object MySQLUtils {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 删除mysql的数据
    * @param sqlContext
    * @param delete_sql
    */
  def deleteMysqlTableData(sqlContext: SQLContext,delete_sql:String): Boolean ={
    val conn =MysqlPoolManager.getMysqlManager.getConnection
    val preparedStatement = conn.createStatement()
    try{
      preparedStatement.execute(delete_sql)
    }catch {
      case e:Exception=>println(s"mysql deleteMysqlTable error:${e.getMessage}")
        false
    }finally {
      preparedStatement.close()
      conn.close()
    }
  }
}
