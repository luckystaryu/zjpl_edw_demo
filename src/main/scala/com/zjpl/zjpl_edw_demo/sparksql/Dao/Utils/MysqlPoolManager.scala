package com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config

object MysqlPoolManager {
  var mysqlManager:MysqlPool =_
  def getMysqlManager:MysqlPool ={
    synchronized{
      if(mysqlManager ==null){
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }

  class MysqlPool extends Serializable{
    private val dds :DruidDataSource =new DruidDataSource()
    try{
      dds.setDriverClassName(edw_config.properties.getProperty("mysql_zjt.driver"))
      dds.setUrl(edw_config.properties.getProperty("mysql_zjt.url"))
      dds.setUsername(edw_config.properties.getProperty("mysql_zjt.user"))
      dds.setPassword(edw_config.properties.getProperty("mysql_zjt.password"))
      dds.setInitialSize(edw_config.properties.getProperty("mysql_zjt.initSize").toInt)
      dds.setMinIdle(edw_config.properties.getProperty("mysql_zjt.minIdle").toInt)
      dds.setMaxActive(edw_config.properties.getProperty("mysql_zjt.maxActive").toInt)
      dds.setMaxWait(edw_config.properties.getProperty("mysql_zjt.maxWait").toInt)
      dds.setTimeBetweenEvictionRunsMillis(edw_config.properties.getProperty("mysql_zjt.timeBetweenEvictionRunsMillis").toInt)
      dds.setMinEvictableIdleTimeMillis(edw_config.properties.getProperty("mysql_zjt.minEvictableIdleTimeMillis").toInt)
      dds.setValidationQuery(edw_config.properties.getProperty("mysql_zjt.validationQuery"))
      dds.setTestWhileIdle(edw_config.properties.getProperty("mysql_zjt.testWhileIdle").toBoolean)
      dds.setMaxPoolPreparedStatementPerConnectionSize(edw_config.properties.getProperty("mysql_zjt.maxPoolPreparedStatementPerConnectionSize").toInt)
    }catch{
      case e:Exception => e.printStackTrace
    }
    def getConnection:Connection={
     try{
       dds.getConnection()
       }catch {
       case e:Exception =>e.printStackTrace()
        null
     }
    }
    def close(): Unit ={
      try{
        dds.close()
      }catch {
        case e:Exception =>e.printStackTrace()
      }
    }
  }
}
