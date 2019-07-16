package com.zjpl.zjpl_edw_demo.dm.hbase

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import utils.MD5Utils

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object hive2hbase {
  val logger:Logger=LoggerFactory.getLogger(this.getClass)
  val driverName:String= "org.apache.hive.jdbc.HiveDriver"

  /**
    * 创建Hbase表
    * @param tableTable
    * @param hBaseConf
    */
  def createHTable(tableTable: String, hBaseConf: Configuration): Unit = {
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

  def startColumnsDescRow(rs: ResultSet) ={
    val colName = rs.getString("col_name")
    colName.trim()=="# col_name"
  }

  def getColumnList(hiveDatabase: String, hiveTable: String, hiveIgoreFields: String) = {
    Class.forName(driverName).newInstance()
    val conn =DriverManager.getConnection("jdbc:hive2://master:10000/default","","")
    var columnList = new ListBuffer[String]
    val ps =conn.prepareStatement("DESC FORMATTED "+hiveDatabase+"."+hiveTable)
    val rs =ps.executeQuery

    val breakWhile = new Breaks
    val continueWhile = new Breaks
    val ignoreList = hiveIgoreFields.split(",").toList
    while(rs.next()){
      if(startColumnsDescRow(rs)){
        breakWhile.breakable{
          while(rs.next()){
            continueWhile.breakable{
              val colName =rs.getString("col_name")
              if(colName == null || colName.trim().equals("")||ignoreList.contains(colName)){
                continueWhile.break()
              }else{
                columnList.append(colName)
              }
            }
          }
        }
      }
    }
    if(conn !=null) conn.close()
    columnList
  }

  def getSelectColumns(columnList: ListBuffer[String]): _root_.scala.Predef.String = {
    var columns = new StringBuilder()
    for(column <- columnList){
      columns.append(column)
      columns.append(",")
    }
    columns.deleteCharAt(columns.length -1).toString()
  }

  def getSql(hiveDatabase: String, hiveTable: String, hivePartition: String, columns: String) = {
    var sql= new StringBuilder()
      .append("SELECT")
      .append(columns)
      .append(" FROM ")
      .append(hiveDatabase)
      .append(".")
      .append(hiveTable)
      .append(" WHERE ")
      .append(" etl_dt ="+"'"+hivePartition+"'")
    sql.toString()
  }

  def main(args:Array[String]): Unit ={

    if(args.length <3){
      throw new IllegalArgumentException("Please input parameters.【hiveDatabase hiveTable hiveIgnoreFields [hivePartition]】")
    }
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = Calendar.getInstance()

    val hiveDatabase:String = args(0)
    val hiveTable:String = args(1)
    val hiveIgoreFields:String=args(2)
    var hivePartition:String=null
    if (args.length ==4){
      hivePartition ==args(3)
    }else{
      date.add(Calendar.DAY_OF_MONTH,-1)
      hivePartition = formatter.format(date.getTime)
    }
    val quorum =edw_config.properties.getProperty("dev_zookeeper.server")
    val clientPort =edw_config.properties.getProperty("dev_zookeeper.port")
    //4.连接hbase
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum",quorum)
    hBaseConf.set("hbase.zookeeper.property.clientPort",clientPort)
    //5.判断表是否存在，表不存在则建Hbase临时表
    createHTable(hiveTable,hBaseConf)
    //获取hive中列
    val columnList =getColumnList(hiveDatabase,hiveTable,hiveIgoreFields)
    //拼接sql
    val columns:String = getSelectColumns(columnList)
    val sql =getSql(hiveDatabase,hiveTable,hivePartition,columns)

    //查询数据
    val spark =SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val dataDF = spark.sql(sql).toDF()

    //写入数据到hbase
    dataDF.foreachPartition(it=>{
      val conn = ConnectionFactory.createConnection(hBaseConf)
      val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
      val tableName= TableName.valueOf(hiveTable)
      val table = conn.getTable(tableName)
      it.foreach(row=>{
        def checkValue(v:Any):String ={
          if(v==null||v.toString.trim.eq("")){
            "null"
          }else{
            v.toString
          }}
          val rowkey = MD5Utils.String2DM5(row(0).toString).getBytes()
          val hBase_Column_Family:String="info"
          val columnFamily =hBase_Column_Family.getBytes()
          val put = new Put(rowkey)
          for(i <- 0 until columnList.size){
            put.addColumn(columnFamily,columnList(i).getBytes,checkValue(row(i)).getBytes())
          }
          table.put(put)
        })
        conn.close()
      })
    }
}
