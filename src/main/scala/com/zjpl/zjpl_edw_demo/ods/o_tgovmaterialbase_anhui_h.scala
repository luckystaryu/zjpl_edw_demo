package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_tgovmaterialbase_anhui_h {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
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
         |create table if not exists ods_db.o_tgovmaterialbase_anhui_h
         |(  Id               int            comment  '自增长主键'
         |,cid             string          comment '分类'
         |,subcid          string          comment '二级分类'
         |,nid             string          comment ''
         |,Addr            string          comment '地址'
         |,Code            string          comment ''
         |,Code03          string          comment ''
         |,Cname           string          comment ''
         |,Name            string          comment '材料名称'
         |,stdName         string          comment '标准名称'
         |,fenci           string          comment '分词匹配项'
         |,Spec            string          comment '规格型号'
         |,Unit            string          comment '单位'
         |,PriceM          decimal         comment '面价'
         |,FID             string          comment '供应商id'
         |,FName           string          comment '供应商名称'
         |,Brand           string          comment '品牌'
         |,IssueDate       timestamp       comment ''
         |,CreateON        timestamp       comment '创建时间'
         |,CreateBy        string          comment '创建人'
         |,UpdateOn        timestamp       comment '更新时间'
         |,UpdateBy        string          comment '更新者'
         |,areacode        string          comment '地区'
         |,Code06          string          comment ''
         |,Code10          string          comment ''
         |,Notes           string          comment '备注'
         |,PriceShow       string          comment '显示的价格'
         |,NameSpec        string          comment '名称规格'
         |,isGov           int             comment '是否信息价'
         |,imageURL        string          comment '价格图片路径'
         |,bianma          string          comment '编码'
         |,features        string          comment '特征项'
         |,keyFeatures     string          comment '关键特征项'
         |,f01             string          comment '特征1'
         |,f02             string          comment '特征2'
         |,f03             string          comment '特征3'
         |,f04             string          comment '特征4'
         |,f05             string          comment '特征5'
         |,f06             string          comment '特征6'
         |,f07             string          comment '特征7'
         |,f08             string          comment '特征8'
         |,f09             string          comment '特征9'
         |,f10             string          comment '特征10'
         |,f11             string          comment '特征11'
         |,f12             string          comment '特征12'
         |,f13             string          comment '特征13'
         |,f14             string          comment '特征14'
         |,f15             string          comment '特征15'
         |,isHandle        int             comment ''
         |,industry        int             comment '行业'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table ods_db.o_tgovmaterialbase_anhui_h drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table ods_db.o_tgovmaterialbase_anhui_h drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table ods_db.o_tgovmaterialbase_anhui_h partition (etl_dt='$yest_dt')
         |select  Id
         |,cid
         |,subcid
         |,nid
         |,Addr
         |,Code
         |,Code03
         |,Cname
         |,Name
         |,stdName
         |,fenci
         |,Spec
         |,Unit
         |,PriceM
         |,FID
         |,FName
         |,Brand
         |,IssueDate
         |,CreateON
         |,CreateBy
         |,UpdateOn
         |,UpdateBy
         |,areacode
         |,Code06
         |,Code10
         |,Notes
         |,PriceShow
         |,NameSpec
         |,isGov
         |,imageURL
         |,bianma
         |,features
         |,keyFeatures
         |,f01
         |,f02
         |,f03
         |,f04
         |,f05
         |,f06
         |,f07
         |,f08
         |,f09
         |,f10
         |,f11
         |,f12
         |,f13
         |,f14
         |,f15
         |,isHandle
         |,industry
         |  from  ods_db.o_tgovmaterialbase_anhui
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
