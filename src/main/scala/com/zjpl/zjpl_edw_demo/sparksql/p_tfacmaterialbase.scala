package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_tfacmaterialbase {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",200)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |create table if not exists pdw_db.p_tfacmaterialbase
         |(
         | id	int          comment 'id'
         |,code	string     comment '材料价格编码'
         |,code2012	string comment '材料编码'
         |,cbianma	string comment ''
         |,cgcid	string   comment '采购一级编码'
         |,cgsubcid	string comment '采购二级编码'
         |,cid	string     comment '一级分类id'
         |,subcid	string   comment '二级分类'
         |,area	string     comment '区域'
         |,province	string
         |,city	string
         |,name	string
         |,stdname	string
         |,fenci	string
         |,spec	string
         |,namespec	string
         |,unit	string
         |,brand	string
         |,addr	string
         |,pricem	decimal(14,4)
         |,priceimage	string
         |,quotjyj	decimal(5,2)
         |,pricej	decimal(14,4)
         |,jyjpriceimage	string
         |,quotgcj	decimal(5,2)
         |,priceg	decimal(14,4)
         |,gcjpriceimage	string
         |,features	string
         |,keyfeatures	string
         |,f01	string
         |,f02	string
         |,f03	string
         |,f04	string
         |,f05	string
         |,f06	string
         |,f07	string
         |,f08	string
         |,f09	string
         |,f10	string
         |,f11	string
         |,f12	string
         |,f13	string
         |,f14	string
         |,f15	string
         |,tid	int
         |,grade	string
         |,fid	string
         |,shoptype	string
         |,ename	string
         |,fname	string
         |,topphoto	string
         |,notes	string
         |,isaudit	string
         |,isdeleted	string
         |,ishandle	int
         |,issuedate	timestamp
         |,createon	timestamp
         |,createby	string
         |,updateon	timestamp
         |,updateby	string
         |,isjournal	tinyint
         |,pagenum	string
         |,title_brand	string
         |,title_namespec	string
         |,title_feature	string
         |,fac_pic	string
         |,price_validday	string
         |,price_validdate	timestamp
         |,description	string
         |,star	string
         |,status	string
         |,sourcetype	string
         |,isformat	tinyint
         |,isrecommend	tinyint
         |,auditremark	string
         |,titlename	string
         |,bid	string
         |,source	tinyint
         |,sort_id	int
         |,boost	decimal(10,2)
         |,es_index	int
         |)partitioned by (etl_dt string)
         |stored as parquet
         """.stripMargin)
    var yest_dt: String = "1990-09-09"
    var dt_date01:String ="1990-09-09"
    var last_month_date:String ="1990-09-09"
    val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
    if (args.length !=0)
    {
      yest_dt = args(0)
      val yest_date=dateForamt.parse(yest_dt)
      val cal_parse =Calendar.getInstance()
      val cal_parse01 =Calendar.getInstance()
      cal_parse.setTime(yest_date)
      cal_parse.add(Calendar.DATE,-1)
      cal_parse01.setTime(yest_date)
      cal_parse01.add(Calendar.MONTH,-1)
      dt_date01=dateForamt.format(cal_parse.getTime)
      last_month_date=dateForamt.format(cal_parse01.getTime)
    }else
    {
      val cal = Calendar.getInstance()
      cal.setTime(new Date())
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE,-1)
      dt_date01 =dateForamt.format(cal.getTime)
      val last_month = Calendar.getInstance()
      last_month.add(Calendar.DATE,-1)
      last_month.add(Calendar.MONTH,-1)
      last_month_date = dateForamt.format(last_month.getTime)
    }
    sql("alter table pdw_db.p_tfacmaterialbase drop if exists partition (etl_dt ='"+yest_dt+"')")
    val exec_sql=sql(
      s"""
         |insert into table pdw_db.p_tfacmaterialbase partition (etl_dt='$yest_dt')
         |select t1.id
         |,t1.code
         |,t1.code2012
         |,t1.cbianma
         |,t1.cgCid
         |,t1.cgSubCid
         |,t1.cid
         |,t1.subcid
         |,t1.area
         |,t1.province
         |,t1.city
         |,t1.name
         |,t1.stdName
         |,t1.fenci
         |,t1.spec
         |,t1.namespec
         |,t1.unit
         |,t1.brand
         |,t1.addr
         |,t1.pricem
         |,t1.priceImage
         |,t1.quotjyj
         |,t1.pricej
         |,t1.jyjPriceImage
         |,t1.quotgcj
         |,t1.priceg
         |,t1.gcjPriceImage
         |,t1.features
         |,t1.keyFeatures
         |,t1.f01
         |,t1.f02
         |,t1.f03
         |,t1.f04
         |,t1.f05
         |,t1.f06
         |,t1.f07
         |,t1.f08
         |,t1.f09
         |,t1.f10
         |,t1.f11
         |,t1.f12
         |,t1.f13
         |,t1.f14
         |,t1.f15
         |,t1.tid
         |,t1.grade
         |,t1.fid
         |,t1.shopType
         |,t1.ename
         |,t1.fname
         |,t1.topPhoto
         |,t1.notes
         |,t1.isAudit
         |,t1.isDeleted
         |,t1.isHandle
         |,t1.issueDate
         |,t1.createON
         |,t1.createBy
         |,t1.updateOn
         |,t1.updateBy
         |,t1.isJournal
         |,t1.pageNum
         |,t1.title_brand
         |,t1.title_namespec
         |,t1.title_feature
         |,t1.fac_pic
         |,t1.price_validDay
         |,t1.price_validDate
         |,t1.description
         |,t1.star
         |,t1.status
         |,t1.sourceType
         |,t1.isFormat
         |,t1.isRecommend
         |,t1.auditRemark
         |,t1.titleName
         |,t1.bid
         |,t1.source
         |,t1.sort_id
         |,t1.boost
         |,t1.es_index
         |from odm_db.o_tfacmaterialbase t1
         |left join ods_db.o_tfacmaterialbase_chk t2
         |  on t1.code = t2.code
         |where t2.code is null
         """.stripMargin)
    logger.warn("exec_sql"+exec_sql)
    spark.stop()
  }
}
