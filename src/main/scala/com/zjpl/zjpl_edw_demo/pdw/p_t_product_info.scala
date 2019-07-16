package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_product_info {
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
         | create table if not exists pdw_db.p_t_product_info
         |(id                   int        comment '产品序号id'
         |,product_num          string     comment '产品编码'
         |,product_name         string     comment '产品名称'
         |,spec_modes           string     comment '产品规格型号'
         |,material_num         string     comment '材料编码'
         |,supplier_num         string     comment '供应商编号'
         |,brand_num            string     comment '品牌编号'
         |,brand_name           string     comment '品牌名称'
         |,buid_industy_cd      string     comment '建筑行业大类代码'
         |,product_description  string     comment '产品详情'
         |,product_logo         string     comment '产品图片'
         |,create_time          timestamp  comment '创建时间'
         |,creator_name         string     comment '创建人'
         |,update_time          timestamp  comment '修改时间'
         |,updator_name         string     comment '修改人'
         |)partitioned by (etl_dt string)
         |stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_product_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table pdw_db.p_t_product_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table pdw_db.p_t_product_info  partition (etl_dt='$yest_dt')
         |select t1.id
         |      ,t1.code
         |      ,t1.name
         |      ,t1.spec
         |      ,t1.code2012
         |      ,t1.fid
         |      ,t2.id
         |      ,t1.brand
         |      ,null as buid_industy_cd
         |      ,t1.description
         |      ,t1.fac_pic
         |      ,t1.createOn
         |      ,t1.createBy
         |      ,t1.updateOn
         |      ,t1.updateBy
         | from ods_db.o_tfacmaterialbase_h t1
         | left join ods_db.o_tfacmatbrand_h t2
         |   on t1.brand = t2.name
         |  and to_date(t2.start_dt) <= to_date('$yest_dt')
         |  and to_date(t2.end_dt) > to_date('$yest_dt')
         |where t1.etl_dt='$yest_dt'
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
