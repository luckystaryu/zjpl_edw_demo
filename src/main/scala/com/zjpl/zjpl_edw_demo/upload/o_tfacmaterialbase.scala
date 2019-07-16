package com.zjpl.zjpl_edw_demo.upload

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils.MySQLUtils
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tfacmaterialbase {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",8000)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    if (args.length != 0) {
      yest_dt = args(0)
    } else {
      val date: Date = new Date()
      val dateForamt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
    }
    import spark.sql
    val rate:Double =scala.util.Random.nextDouble()
    val o_tGovExtends_anhuiDF = sql(
      s"""
         |SELECT
         | id
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
         |,cast(case when pricem is null
         |       then pricem
         |       else (pricem*$rate+100)/100
         |   end as decimal(14,4)) as pricem
         |,priceImage
         |,quotjyj
         |,cast(case when pricej is null
         |      then pricej
         |      else (pricej*$rate+100)/100
         |   end as decimal(14,4)) as pricej
         |,jyjPriceImage
         |,quotgcj
         |,cast(case when priceg is null
         |       then priceg
         |       else (priceg*$rate+100)/100
         |   end as decimal(14,4)) as priceg
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
         |,cast(issueDate as timestamp) as issueDate
         |,cast(createON as timestamp) as createON
         |,createBy
         |,cast(updateOn as timestamp) as updateOn
         |,updateBy
         |,isJournal
         |,pageNum
         |,title_brand
         |,title_namespec
         |,title_feature
         |,fac_pic
         |,price_validDay
         |,cast(price_validDate as timestamp) as price_validDate
         |,description
         |,star
         |,status
         |,sourceType
         |,isFormat
         |,isRecommend
         |,auditRemark
         |,titleName
         |,bid
         |,sort_id as `sort`
         |,boost
         |,es_index
         | from ods_db.o_tfacmaterialbase
    """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjttest.url")
    val user = edw_config.properties.getProperty("mysql_zjttest.user")
    val password = edw_config.properties.getProperty("mysql_zjttest.password")
//    val tFacMaterialBaseSQL=
//      s"""
//         |  `id` int(11) NOT NULL,
//         |  `code` char(13) DEFAULT '',
//         |  `code2012` char(8) DEFAULT '',
//         |  `cbianma` char(11) DEFAULT '',
//         |  `cgCid` char(2) DEFAULT '',
//         |  `cgSubCid` char(4) DEFAULT '',
//         |  `cid` char(2) DEFAULT '',
//         |  `subcid` char(4) DEFAULT '',
//         |  `area` varchar(18) DEFAULT '',
//         |  `province` varchar(9) DEFAULT '',
//         |  `city` varchar(18) DEFAULT '',
//         |  `name` varchar(90) DEFAULT '',
//         |  `stdName` varchar(64) DEFAULT '' COMMENT '标准名称',
//         |  `fenci` varchar(255) DEFAULT NULL,
//         |  `spec` varchar(210) DEFAULT '',
//         |  `namespec` varchar(330) DEFAULT '',
//         |  `unit` varchar(18) DEFAULT '',
//         |  `brand` varchar(18) DEFAULT '',
//         |  `addr` varchar(20) DEFAULT '',
//         |  `pricem` decimal(14,4) DEFAULT '0.0000',
//         |  `priceImage` varchar(128) DEFAULT NULL,
//         |  `quotjyj` decimal(5,2) DEFAULT '0.00',
//         |  `pricej` decimal(14,4) DEFAULT '0.0000',
//         |  `jyjPriceImage` varchar(128) DEFAULT NULL,
//         |  `quotgcj` decimal(5,2) DEFAULT '0.00',
//         |  `priceg` decimal(14,4) DEFAULT '0.0000',
//         |  `gcjPriceImage` varchar(128) DEFAULT NULL,
//         |  `features` varchar(2000) DEFAULT '',
//         |  `keyFeatures` varchar(2000) DEFAULT '' COMMENT '关键特征集',
//         |  `f01` varchar(256) DEFAULT '',
//         |  `f02` varchar(256) DEFAULT '',
//         |  `f03` varchar(256) DEFAULT '',
//         |  `f04` varchar(256) DEFAULT '',
//         |  `f05` varchar(256) DEFAULT '',
//         |  `f06` varchar(256) DEFAULT '',
//         |  `f07` varchar(256) DEFAULT '',
//         |  `f08` varchar(256) DEFAULT '',
//         |  `f09` varchar(256) DEFAULT '',
//         |  `f10` varchar(256) DEFAULT '',
//         |  `f11` varchar(256) DEFAULT '',
//         |  `f12` varchar(256) DEFAULT '',
//         |  `f13` varchar(256) DEFAULT '',
//         |  `f14` varchar(256) DEFAULT '',
//         |  `f15` varchar(256) DEFAULT '',
//         |  `tid` int(11) DEFAULT NULL,
//         |  `grade` char(1) DEFAULT '1',
//         |  `fid` varchar(32) DEFAULT '',
//         |  `shopType` char(1) DEFAULT '1' COMMENT '商铺类型(4诚信供应商，3优质供应商，2参考供应商，1普通供应商)',
//         |  `ename` varchar(128) DEFAULT '',
//         |  `fname` varchar(128) DEFAULT '',
//         |  `topPhoto` varchar(60) DEFAULT '',
//         |  `notes` varchar(500) DEFAULT '',
//         |  `isAudit` char(1) DEFAULT '0',
//         |  `isDeleted` char(1) DEFAULT '0',
//         |  `isHandle` int(11) DEFAULT '0',
//         |  `issueDate` datetime NOT NULL,
//         |  `createON` datetime NOT NULL,
//         |  `createBy` varchar(50) NOT NULL,
//         |  `updateOn` datetime NOT NULL,
//         |  `updateBy` varchar(50) NOT NULL,
//         |  `isJournal` tinyint(1) DEFAULT '0',
//         |  `title_brand` varchar(20) DEFAULT NULL COMMENT '供应商后台需求,标题品牌,可能和普通品牌不一致',
//         |  `title_namespec` varchar(200) DEFAULT NULL COMMENT '供应商后台需求,标题名称型号',
//         |  `title_feature` varchar(200) DEFAULT NULL COMMENT '供应商后台需求,标题特征',
//         |  `fac_pic` varchar(200) DEFAULT NULL,
//         |  `price_validDay` varchar(4) DEFAULT NULL COMMENT '产品价格有效天数',
//         |  `price_validDate` datetime DEFAULT NULL COMMENT '产品价格有效日期',
//         |  `description` longtext COMMENT '产品详情',
//         |  `star` char(1) DEFAULT NULL COMMENT '产品星级(1/2/3/4/5)',
//         |  `status` char(1) DEFAULT '1' COMMENT '产品状态，0下架 1上架',
//         |  `sourceType` char(1) NOT NULL DEFAULT '0' COMMENT '材价来源，0：手工录入，S：供应商发布',
//         |  `isFormat` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否标准化(0-否，1-是)',
//         |  `isRecommend` tinyint(1) DEFAULT '0' COMMENT '是否推荐，0-否，1-是',
//         |  `auditRemark` varchar(200) DEFAULT '' COMMENT '审核备注',
//         |  `titleName` varchar(300) DEFAULT '' COMMENT '标题',
//         |  `bid` char(2) DEFAULT '' COMMENT '大类',
//         |  `source` tinyint(3) NOT NULL DEFAULT '1' COMMENT '1:造价通，2:GCW',
//         |  `sort` int(11) DEFAULT '0',
//         |  `boost` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT 'ES搜索权重',
//         |  `es_index` int(2) DEFAULT '5' COMMENT 'ES索引'
//       """.stripMargin
//  val deletesql="truncate table materialfac.tFacMaterialBase"
//  MySQLUtils.deleteMysqlTableData(spark.sqlContext,deletesql)
    val tFacMaterialBaseDF = o_tGovExtends_anhuiDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
     // .option("createTableColumnType",tFacMaterialBaseSQL)
      .mode(SaveMode.Append)
      .option("dbtable","materialfac.tFacMaterialBase_test")
      .save()
    spark.stop()
  }
}
