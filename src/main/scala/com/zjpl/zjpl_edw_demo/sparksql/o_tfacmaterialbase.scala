package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tfacmaterialbase {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    if (args.length !=0)
    {
      yest_dt = args(0)
    }else
    {
      val date:Date = new Date()
      val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
    }
    logger.info(yest_dt)
//    val tFacMaterialBasesql=
//      s"""
//         |(SELECT id
//         |,REPLACE(REPLACE(CODE, '\n', ''), '\r', '') as CODE
//         |,REPLACE(REPLACE(code2012, '\n', ''), '\r', '') as code2012
//         |,REPLACE(REPLACE(cbianma, '\n', ''), '\r', '') as cbianma
//         |,REPLACE(REPLACE(cgCid, '\n', ''), '\r', '') as cgCid
//         |,REPLACE(REPLACE(cgSubCid, '\n', ''), '\r', '') as cgSubCid
//         |,REPLACE(REPLACE(cid, '\n', ''), '\r', '') as cid
//         |,REPLACE(REPLACE(subcid, '\n', ''), '\r', '') as subcid
//         |,REPLACE(REPLACE(area, '\n', ''), '\r', '') as area
//         |,REPLACE(REPLACE(province, '\n', ''), '\r', '') as province
//         |,REPLACE(REPLACE(city, '\n', ''), '\r', '') as city
//         |,REPLACE(REPLACE(NAME, '\n', ''), '\r', '') as NAME
//         |,REPLACE(REPLACE(stdName, '\n', ''), '\r', '') as stdName
//         |,REPLACE(REPLACE(fenci, '\n', ''), '\r', '') as fenci
//         |,REPLACE(REPLACE(spec, '\n', ''), '\r', '') as spec
//         |,REPLACE(REPLACE(namespec, '\n', ''), '\r', '') as namespec
//         |,REPLACE(REPLACE(unit, '\n', ''), '\r', '') as unit
//         |,REPLACE(REPLACE(brand, '\n', ''), '\r', '') as brand
//         |,REPLACE(REPLACE(addr, '\n', ''), '\r', '') as addr
//         |,pricem
//         |,REPLACE(REPLACE(priceImage, '\n', ''), '\r', '') as priceImage
//         |,quotjyj
//         |,pricej
//         |,REPLACE(REPLACE(jyjPriceImage, '\n', ''), '\r', '') as jyjPriceImage
//         |,quotgcj
//         |,priceg
//         |,REPLACE(REPLACE(gcjPriceImage, '\n', ''), '\r', '') as gcjPriceImage
//         |,REPLACE(REPLACE(features, '\n', ''), '\r', '') as features
//         |,REPLACE(REPLACE(keyFeatures, '\n', ''), '\r', '') as keyFeatures
//         |,REPLACE(REPLACE(f01, '\n', ''), '\r', '') as f01
//         |,REPLACE(REPLACE(f02, '\n', ''), '\r', '') as f02
//         |,REPLACE(REPLACE(f03, '\n', ''), '\r', '') as f03
//         |,REPLACE(REPLACE(f04, '\n', ''), '\r', '') as f04
//         |,REPLACE(REPLACE(f05, '\n', ''), '\r', '') as f05
//         |,REPLACE(REPLACE(f06, '\n', ''), '\r', '') as f06
//         |,REPLACE(REPLACE(f07, '\n', ''), '\r', '') as f07
//         |,REPLACE(REPLACE(f08, '\n', ''), '\r', '') as f08
//         |,REPLACE(REPLACE(f09, '\n', ''), '\r', '') as f09
//         |,REPLACE(REPLACE(f10, '\n', ''), '\r', '') as f10
//         |,REPLACE(REPLACE(f11, '\n', ''), '\r', '') as f11
//         |,REPLACE(REPLACE(f12, '\n', ''), '\r', '') as f12
//         |,REPLACE(REPLACE(f13, '\n', ''), '\r', '') as f13
//         |,REPLACE(REPLACE(f14, '\n', ''), '\r', '') as f14
//         |,REPLACE(REPLACE(f15, '\n', ''), '\r', '') as f15
//         |,REPLACE(REPLACE(tid, '\n', ''), '\r', '') as tid
//         |,REPLACE(REPLACE(grade, '\n', ''), '\r', '') as grade
//         |,REPLACE(REPLACE(fid, '\n', ''), '\r', '') as fid
//         |,REPLACE(REPLACE(shopType, '\n', ''), '\r', '') as shopType
//         |,REPLACE(REPLACE(ename, '\n', ''), '\r', '') as ename
//         |,REPLACE(REPLACE(fname, '\n', ''), '\r', '') as fname
//         |,REPLACE(REPLACE(topPhoto, '\n', ''), '\r', '') as topPhoto
//         |,REPLACE(REPLACE(notes, '\n', ''), '\r', '') as notes
//         |,REPLACE(REPLACE(isAudit, '\n', ''), '\r', '') as isAudit
//         |,REPLACE(REPLACE(isDeleted, '\n', ''), '\r', '') as isDeleted
//         |,REPLACE(REPLACE(isHandle, '\n', ''), '\r', '') as isHandle
//         |,issueDate
//         |,createON
//         |,REPLACE(REPLACE(createBy, '\n', ''), '\r', '') as createBy
//         |,updateOn
//         |,REPLACE(REPLACE(updateBy, '\n', ''), '\r', '') as updateBy
//         |,REPLACE(REPLACE(isJournal, '\n', ''), '\r', '') as isJournal
//         |,REPLACE(REPLACE(pageNum, '\n', ''), '\r', '') as pageNum
//         |,REPLACE(REPLACE(title_brand, '\n', ''), '\r', '') as title_brand
//         |,REPLACE(REPLACE(title_namespec, '\n', ''), '\r', '') as title_namespec
//         |,REPLACE(REPLACE(title_feature, '\n', ''), '\r', '') as title_feature
//         |,REPLACE(REPLACE(fac_pic, '\n', ''), '\r', '') as fac_pic
//         |,REPLACE(REPLACE(price_validDay, '\n', ''), '\r', '') as price_validDay
//         |,price_validDate
//         |,REPLACE(REPLACE(description, '\n', ''), '\r', '') as description
//         |,REPLACE(REPLACE(star, '\n', ''), '\r', '') as star
//         |,REPLACE(REPLACE(STATUS, '\n', ''), '\r', '') as STATUS
//         |,REPLACE(REPLACE(sourceType, '\n', ''), '\r', '') as sourceType
//         |,REPLACE(REPLACE(isFormat, '\n', ''), '\r', '') as isFormat
//         |,REPLACE(REPLACE(isRecommend, '\n', ''), '\r', '') as isRecommend
//         |,REPLACE(REPLACE(auditRemark, '\n', ''), '\r', '') as auditRemark
//         |,REPLACE(REPLACE(titleName, '\n', ''), '\r', '') as titleName
//         |,REPLACE(REPLACE(bid, '\n', ''), '\r', '') as bid
//         |,REPLACE(REPLACE(source, '\n', ''), '\r', '') as source
//         |,REPLACE(REPLACE(sort, '\n', ''), '\r', '') as sort
//         |,REPLACE(REPLACE(boost, '\n', ''), '\r', '') as boost
//         |,REPLACE(REPLACE(es_index, '\n', ''), '\r', '') as es_index
//         |from materialfac.tFacMaterialBase
//         |Where cid in('17','18','19','23','28','55','32')) as tFacMaterialBase_tmp
//       """.stripMargin
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password =edw_config.properties.getProperty("mysql_zjt.password")
    val o_tfacmaterialbaseDF =spark.read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","materialfac.tFacMaterialBase")
      .option("qutoALL",true)
      .load()
    o_tfacmaterialbaseDF.write.mode(SaveMode.Overwrite).saveAsTable("ods_db.o_tfacmaterialbase")
    spark.stop()
  }
}
