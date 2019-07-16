package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

/**
  * 材料编码信息
  */
object p_t_material_info {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",800)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    var dateForamt: SimpleDateFormat =null
    var yest_dt02:String = "1990-09-09"
    if (args.length != 0) {
      yest_dt = args(0)
      dateForamt = new SimpleDateFormat("yyyy-MM-dd")
      val yest_dt01:Date =dateForamt.parse(yest_dt)
      val cal= Calendar.getInstance()
      cal.setTime(yest_dt01)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    } else {
      val date: Date = new Date()
      dateForamt= new SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    }
    import spark.sql
    sql(
      s"""
         | create table if not exists pdw_db.p_t_material_info
         |(id	          int             comment '材料id'
         |,material_num	string          comment '材料编码'
         |,material_name	string          comment '材料名称'
         |,material_icon	string          comment '材料图片'
         |,parent_material_num	string    comment '上级材料编码'
         |,is_leaf	string                comment '是否叶子节点'
         |,create_time	timestamp         comment '创建时间'
         |,creator_name	string          comment '创建人'
         |,update_time	timestamp       comment '修改时间'
         |,updator_name	string          comment '修改人'
         |,src_table    string          comment '数据来源表'
         |)stored as parquet
       """.stripMargin)
    sql(
      s"""
         |insert overwrite table pdw_db.p_t_material_info
         |select t1.id
         |      ,t1.code
         |      ,t1.name
         |      ,null
         |      ,t1.pid
         |      ,t1.isLeaf
         |      ,current_timestamp
         |      ,'sys'
         |      ,current_timestamp
         |      ,'sys'
         |      ,'materialfac.tRationLib'
         | from ods_db.o_tRationLib t1
         |union all
         |select t2.id
         |      ,t2.code
         |      ,t2.name
         |      ,t2.image
         |      ,t2.subcid
         |      ,'1'
         |      ,current_timestamp
         |      ,'sys'
         |      ,current_timestamp
         |      ,'sys'
         |      ,'materialfac.tMaterialBaseLib'
         | from ods_db.o_tMaterialBaseLib t2
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
