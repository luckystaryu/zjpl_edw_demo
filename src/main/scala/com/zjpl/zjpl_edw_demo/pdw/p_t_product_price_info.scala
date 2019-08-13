package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.spark_udf.customUDF
import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_product_price_info {
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
    spark.udf.register("cityNameUDF",customUDF.cityNameUDF(_:String))
    sql(
      s"""
         | create table if not exists pdw_db.p_t_product_price_info
         |(id                int		        comment '序号'
         |,product_num       string        comment '产品编码'
         |,product_amount    decimal(18,2) comment '产品数量'
         |,unit              string        comment '单位'
         |,tax_rate          decimal(6,4)  comment '税率'
         |,tax_price         decimal(18,4) comment '含税价格'
         |,currency_cd       string        comment '货币代码'
         |,supplier_num      string        comment '供应商编号'
         |,issue_date        string        comment '发布日期'
         |,valide_date       string        comment '有效日期'
         |,area_cd           string        comment '发布行政区划代码'
         |,price_class_cd    string        comment '价格分类代码'
         |,price_provider    string        comment '价格提供者'
         |,create_time          timestamp  comment '创建时间'
         |,creator_name         string     comment '创建人'
         |,update_time          timestamp  comment '修改时间'
         |,updator_name         string     comment '修改人'
         |)partitioned by (etl_dt string)
         |stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_product_price_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table pdw_db.p_t_product_price_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table pdw_db.p_t_product_price_info  partition (etl_dt='$yest_dt')
         |select t1.id
         |      ,t1.code
         |      ,null as product_amount
         |      ,t1.unit
         |      ,0 as tax_rate
         |      ,t1.pricem
         |      ,'CNY' as currency_cd
         |      ,t1.fid
         |      ,cast(to_date(t1.issuedate) as string)
         |      ,t1.price_validday
         |      ,coalesce(t2.regionCode,t3.regionCode,t1.city) as regionCode
         |      ,'02' as price_class_cd
         |      ,t1.source
         |      ,t1.createOn
         |      ,t1.createBy
         |      ,t1.updateOn
         |      ,t1.updateBy
         | from ods_db.o_tfacmaterialbase_h t1
         | left join ods_db.o_tareamanagement_h t2
         |  on t1.city = t2.name
         | and length(t2.regioncode)=6
         | and to_date(t2.start_dt) <= to_date('$yest_dt')
         | and to_date(t2.end_dt) > to_date('$yest_dt')
         | left join ods_db.o_tareamanagement_h t3
         |  on cityNameUDF(t1.city) = t3.name
         | and length(t3.regioncode)=6
         | and to_date(t3.start_dt) <= to_date('$yest_dt')
         | and to_date(t3.end_dt) > to_date('$yest_dt')
         |where t1.etl_dt='$yest_dt'
         |  and t1.isDeleted !='1'
         |  and t1.isAudit ='1'
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
