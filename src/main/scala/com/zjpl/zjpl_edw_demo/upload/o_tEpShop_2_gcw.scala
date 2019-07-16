package com.zjpl.zjpl_edw_demo.upload

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils.MySQLUtils
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tEpShop_2_gcw {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
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
    val rate:Double =scala.util.Random.nextDouble();
    val o_tEpShop_2_gcwDF = sql(
      s"""
         |select  t1.id
         |,t1.EID
         |,t1.Name
         |,t1.Fname
         |,t1.addr
         |,t1.postCode
         |,t1.homePage
         |,t1.corpn
         |,t1.area
         |,t1.logo
         |,t1.longLogo
         |,t1.adURL
         |,t1.culture
         |,t1.bglogo
         |,t1.linkman
         |,t1.mainMaterial
         |,t1.saleArea
         |,t1.branchInfo
         |,t1.provSort
         |,t1.province
         |,t1.city
         |,t1.picPath
         |,t1.sqsPath
         |,t1.brand
         |,t1.discription
         |,t1.regArea
         |,t1.regAddress
         |,t1.regCapital
         |,cast(t1.regTime as timestamp) as regTime
         |,t1.regOffice
         |,cast(t1.startTime as timestamp) as startTime
         |,cast(endTime as timestamp) as endTime
         |,t1.regNum
         |,t1.businessScope
         |,t1.businessLicensePic
         |,t1.businessMode
         |,t1.enterpriseType
         |,t1.ShopType
         |,t1.Cname
         |,t1.Cname1
         |,cast(t1.CreateOn as timestamp) as CreateOn
         |,t1.CreateBy
         |,cast(t1.UpdateOn as timestamp) as UpdateOn
         |,t1.UpdateBy
         |,t1.IsLock
         |,t1.IsAudit
         |,t1.isDeleted
         |,t1.Auditor
         |,cast(t1.effectDate as timestamp) as effectDate
         |,cast(t1.auditDate as timestamp) as auditDate
         |,cast(t1.ValidDate as timestamp) as ValidDate
         |,t1.isVolid
         |,t1.`Sort`
         |,t1.ManageID
         |,t1.Degree
         |,t1.cid
         |,t1.subcid
         |,t1.catalogId
         |,t1.grade
         |,t1.hasUnAuditMat
         |,t1.matCount
         |,t1.contact
         |,t1.sex
         |,t1.department
         |,t1.mobile
         |,t1.phone
         |,t1.isIntegrity
         |,t1.creditScore
         |,t1.authContent
         |,t1.recommendContent
         |,t1.putCount
         |,t1.certifNum
         |,t1.fax
         |,t1.integrityLogo
         |,t1.isTop
         |,t1.refCount
         |,cast(t1.matUpdateOn as timestamp) as matUpdateOn
         |,t1.cgCid
         |,t1.cgSubCid
         |,cast(t1.publishDate as timestamp) as publishDate
         |,t1.isFlagship
         |,t1.isHonourable
         |,t1.isJournal
         |,t1.association
         |,t1.stdNames
         |,t1.memberId
         |,t1.regProvince
         |,t1.regCity
         |,t1.isCertified
         |,to_date(t1.creationDate) as creationDate
         |,t1.refreshMatFlag
         |,t1.mainProductText
         |,t1.isVip
         |,cast(t1.vipTime as timestamp) as vipTime
         |,cast(t1.certifiedTime as timestamp) as certifiedTime
         |,t1.fromType
         |,t1.registerIp
         |,t1.registerAddr
         |,t1.source
         |,t1.projectViewNum
         |,t1.guideFlag
         |,t1.isTest
         |,t1.regCapitalUnit
         |,t1.mail
         |,t1.upgradeTips
         |,t1.bid
         |,t1.regCapitalRmb
         |,t1.userType
         |,t1.levelCode
         |,t1.projectViewProvince
         |,t1.creditCode
         |,t1.isEnvironmental
         |,t1.qq
         |,t1.boost
         |,t1.synthesisScore
         |,t1.tmpStatus
         | from ods_db.o_tEpShop t1
         | left join ods_db.o_tianyancha_supplier t2
         |   on t1.name = t2.search_name
         |  left join ods_db.o_tianyancha_supplier_20190614 t3
         |   on t1.name = t3.search_name
         |where t1.isDeleted!='1'
         |  AND t1.IsAudit='1'
         |  AND t2.search_name is null
         |  AND t3.search_name is null
       """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password = edw_config.properties.getProperty("mysql_zjt.password")
    //val deletesql="truncate table materialgov.tGovExtends_anhui"
    //MySQLUtils.deleteMysqlTableData(spark.sqlContext,deletesql)
    val mysteel_dataDF = o_tEpShop_2_gcwDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .option("dbtable","gcw.tEpShop")
      .save()
    spark.stop()
  }
}
