package com.zjpl.zjpl_edw_demo.hbase

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import utils.MD5Utils

import scala.util.control.Breaks._

object hive2hbase  {
  val logger:Logger=LoggerFactory.getLogger(this.getClass)
  def main(args:Array[String]): Unit ={

    /**
      * 传入参数处理
      */
    if(args.length <2){
      throw new IllegalArgumentException("Please input parameters.【hiveDatabase hiveTable [hiveIgnoreFields] [hivePartition]】")
    }
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = Calendar.getInstance()

    val hiveDatabase:String = args(0)
    val hiveTable:String = args(1)
    var hiveIgoreFields:String=""
    var hivePartition:String=""
    logger.warn("传入参数长度："+args.length)
    if (args.length ==3){
      hivePartition =args(2)
    }
    if (args.length ==4){
      hiveIgoreFields =args(2)
      logger.warn("hiveIgoreFields================================================================"+hiveIgoreFields)
      hivePartition =args(3)
      logger.warn("hivePartition================================================================"+hivePartition)
    }else{
      date.add(Calendar.DAY_OF_MONTH,-1)
      hivePartition = formatter.format(date.getTime)
      logger.warn("hivePartition01================================================================"+hivePartition)
    }
    /**
      * hbase 参数获取和连接
      */
    val quorum =edw_config.properties.getProperty("dev_zookeeper.server")
    val clientPort =edw_config.properties.getProperty("dev_zookeeper.port")
    //4.连接hbase
    //获取hive中列
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark =SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
     // .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.debug.maxToStringFields",800)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    val table_sql =sql(
      s"""
         |describe $hiveDatabase.$hiveTable
       """.stripMargin)
    val columnLists = table_sql.select("col_name").collectAsList()
    var columnList:String =""
    breakable{
      for (i <- 0 until columnLists.size()) {
        if (StringUtils.isNotBlank(columnList)) {
          if (columnLists.get(i).get(0).toString.contains("#")) {
            break()
          } else {
            columnList = columnList.concat(",").concat(columnLists.get(i).get(0).toString)
          }
        } else {
          columnList = columnLists.get(i).get(0).toString
        }
      }
    }


    //拼接sql
    val v_sql =getSql(hiveDatabase,hiveTable,hivePartition,columnList)
    logger.warn(v_sql)

    //查询数据

    val dataDF = sql(v_sql).toDF()
    val dataAmount=dataDF.count()
    val perNum:Int=50000000
    var regionNum:Int=(dataAmount/perNum).toInt
    //5.判断表是否存在，表不存在则建Hbase临时表
    createHTable(hiveTable,quorum,clientPort)
    //写入数据到hbase
    dataDF.foreachPartition(it=>{
      val hBaseConf = HBaseConfiguration.create()
      hBaseConf.set("hbase.zookeeper.quorum",quorum)
      hBaseConf.set("hbase.zookeeper.property.clientPort",clientPort)
      val conn = ConnectionFactory.createConnection(hBaseConf)
      val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
      val tableName= TableName.valueOf(hiveTable)
      val table = conn.getTable(tableName)
      it.foreach(row=> {
        def checkValue(v: Any): String = {
          if (v == null || v.toString.trim.eq("")) {
            "null"
          } else {
            v.toString
          }
        }

        val rowkey = MD5Utils.String2DM5(row(0).toString).getBytes()
        val hBase_Column_Family: String = "info"
        val columnFamily = hBase_Column_Family.getBytes()
        val put = new Put(rowkey)
        breakable {
          for (k <- 0 until columnLists.size()) {
              if (StringUtils.isNotBlank(columnList)&&columnLists.get(k).get(0).toString.contains("#")) {
                break()
              } else {
                columnList =columnLists.get(k).get(0).toString
              }
            put.addColumn(columnFamily, Bytes.toBytes(columnList), Bytes.toBytes(checkValue(row(k))))
          }
        }
        table.put(put)
      })
      conn.close()
    })
  }

  /**
    *
    * @param tableTable
    * @param quorum
    * @param clientPort
    */
  def createHTable(tableTable: String,quorum:String,clientPort:String): Unit = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum",quorum)
    hBaseConf.set("hbase.zookeeper.property.clientPort",clientPort)
    val connection =ConnectionFactory.createConnection(hBaseConf)
    val hBaseTableName = TableName.valueOf(tableTable)
    val admin = connection.getAdmin
    if(!admin.tableExists(hBaseTableName)){
      val tableDesc =new HTableDescriptor(hBaseTableName)
      val hBase_Column_Family:String="info"
      val columnDesc = new HColumnDescriptor(hBase_Column_Family)
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)
    }
    connection.close()
  }

  def getSql(hiveDatabase: String, hiveTable: String, hivePartition: String, columns: String) = {
    logger.warn("hivePartition================================================================"+hivePartition)
    var sql= new StringBuilder()
      .append(" SELECT ")
      .append(columns)
      .append(" FROM ")
      .append(hiveDatabase)
      .append(".")
      .append(hiveTable)
      .append(" WHERE ")
      .append(" etl_dt ="+"'"+ hivePartition +"'")
    sql.toString()
  }
}
