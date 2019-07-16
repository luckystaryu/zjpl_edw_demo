package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_tfacmaterialbase_h {
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
         |create table if not exists ods_db.o_tfacmaterialbase_h
         |(id                int             comment'自增长主键'
         |,code              string          comment'市场价材料编码'
         |,code2012          string          comment'2012编码'
         |,cbianma           string          comment'书刊编码'
         |,cgCid             string          comment'采购二级分类id'
         |,cgSubCid          string          comment'采购二级分类'
         |,cid               string          comment'一级分类id'
         |,subcid            string          comment'二级分类id'
         |,area              string          comment'区域'
         |,province          string          comment'省份'
         |,city              string          comment'市'
         |,name              string          comment'名称'
         |,stdName           string          comment'标准名称'
         |,fenci             string          comment'分词匹配项'
         |,spec              string          comment'规格'
         |,namespec          string          comment'产品名称加型号规格'
         |,unit              string          comment'单位'
         |,brand             string          comment'品牌'
         |,addr              string          comment'地址'
         |,pricem            decimal(14,4)   comment'材料价格'
         |,priceImage        string          comment'价格图片路径'
         |,quotjyj           decimal(5,2)    comment'建议价系数'
         |,pricej            decimal(14,4)   comment'建议价'
         |,jyjPriceImage     string          comment'建议价图片路径'
         |,quotgcj           decimal(5,2)    comment'工程价系数'
         |,priceg            decimal(14,4)   comment'工程价'
         |,gcjPriceImage     string          comment'工程价价格图片路径'
         |,features          string          comment'特征'
         |,keyFeatures       string          comment'关键特征集'
         |,f01               string          comment'特征值'
         |,f02               string          comment'特征值'
         |,f03               string          comment'特征值'
         |,f04               string          comment'特征值'
         |,f05               string          comment'特征值'
         |,f06               string          comment'特征值'
         |,f07               string          comment'特征值'
         |,f08               string          comment'特征值'
         |,f09               string          comment'特征值'
         |,f10               string          comment'特征值'
         |,f11               string          comment'特征值'
         |,f12               string          comment'特征值'
         |,f13               string          comment'特征值'
         |,f14               string          comment'特征值'
         |,f15               string          comment'特征值'
         |,tid               int             comment''
         |,grade             string          comment'等级'
         |,fid               string          comment'企业id'
         |,shopType          string          comment'商铺类型'
         |,ename             string          comment'企业名称'
         |,fname             string          comment'企业名称'
         |,topPhoto          string          comment'顶部图片'
         |,notes             string          comment'备注'
         |,isAudit           string          comment'审核状态'
         |,isDeleted         string          comment'是否删除'
         |,isHandle          int             comment'处理标识'
         |,issueDate         timestamp        comment'发布日期'
         |,createON          timestamp        comment'创建时间'
         |,createBy          string          comment'创建人'
         |,updateOn          timestamp        comment'更新时间'
         |,updateBy          string          comment'更新人'
         |,isJournal         tinyint         comment'是否是期刊价  0否 1是'
         |,pageNum           string          comment'页码'
         |,title_brand       string          comment'供应商后台需求,标题品牌,可能和普通品牌不一致'
         |,title_namespec    string          comment'供应商后台需求,标题名称型号'
         |,title_feature     string          comment'供应商后台需求,标题特征'
         |,fac_pic           string          comment'供应商后台需求,产品图片地址'
         |,price_validDay    string          comment'产品价格有效天数'
         |,price_validDate   timestamp       comment'产品价格有效日期'
         |,description       string          comment'产品详情'
         |,star              string          comment'产品星级'
         |,status            string          comment'产品状态'
         |,sourceType        string          comment'材价来源'
         |,isFormat          tinyint         comment'是否标准化'
         |,isRecommend       tinyint         comment'是否推荐'
         |,auditRemark       string          comment'审核备注'
         |,titleName         string          comment'标题'
         |,bid               string          comment'大类'
         |,source            tinyint         comment'1:造价通，2:GCW'
         |,sort_id           int             comment'排序'
         |,boost             decimal(10,2)   comment'ES搜索权重'
         |,es_index          int             comment'ES索引'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table ods_db.o_tfacmaterialbase_h drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table ods_db.o_tfacmaterialbase_h drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table ods_db.o_tfacmaterialbase_h partition (etl_dt='$yest_dt')
         |select id
         |,code
         |,code2012
         |,cbianma
         |,cgCid
         |,cgSubCid
         |,cid
         |,subcid
         |,area
         |,province
         |,city
         |,name
         |,stdName
         |,fenci
         |,spec
         |,namespec
         |,unit
         |,brand
         |,addr
         |,pricem
         |,priceImage
         |,quotjyj
         |,pricej
         |,jyjPriceImage
         |,quotgcj
         |,priceg
         |,gcjPriceImage
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
         |,tid
         |,grade
         |,fid
         |,shopType
         |,ename
         |,fname
         |,topPhoto
         |,notes
         |,isAudit
         |,isDeleted
         |,isHandle
         |,issueDate
         |,createON
         |,createBy
         |,updateOn
         |,updateBy
         |,isJournal
         |,pageNum
         |,title_brand
         |,title_namespec
         |,title_feature
         |,fac_pic
         |,price_validDay
         |,price_validDate
         |,description
         |,star
         |,status
         |,sourceType
         |,isFormat
         |,isRecommend
         |,auditRemark
         |,titleName
         |,bid
         |,source
         |,sort_id
         |,boost
         |,es_index
         | from ods_db.o_tfacmaterialbase
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
