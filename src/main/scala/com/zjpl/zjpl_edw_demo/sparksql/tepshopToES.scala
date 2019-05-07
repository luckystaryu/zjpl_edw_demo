package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

object tepshopToES {
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val tepshopToESDF = spark.sql(
      s"""
         |SELECT
         | id
         |,eid
         |,name
         |,addr
         |,postcode
         |,homepage
         |,corpn
         |,area
         |,logo
         |,longlogo
         |,adurl
         |,culture
         |,bglogo
         |,linkman
         |,mainmaterial
         |,salearea
         |,branchinfo
         |,provsort
         |,province
         |,city
         |,picpath
         |,sqspath
         |,brand
         |,discription
         |,regarea
         |,regaddress
         |,cast(regcapital as string) as regcapital
         |,regtime
         |,regoffice
         |,starttime
         |,endtime
         |,regnum
         |,businessscope
         |,businesslicensepic
         |,businessmode
         |,enterprisetype
         |,shoptype
         |,createon
         |,createby
         |,updateon
         |,updateby
         |,islock
         |,isaudit
         |,isdeleted
         |,auditor
         |,effectdate
         |,auditdate
         |,validdate
         |,isvolid
         |,manageid
         |,degree
         |,cid
         |,subcid
         |,catalogid
         |,grade
         |,hasunauditmat
         |,matcount
         |,contact
         |,sex
         |,department
         |,mobile
         |,phone
         |,isintegrity
         |,creditscore
         |,authcontent
         |,recommendcontent
         |,putcount
         |,certifnum
         |,fax
         |,integritylogo
         |,istop
         |,refcount
         |,matupdateon
         |,publishdate
         |,isflagship
         |,ishonourable
         |,isjournal
         |,association
         |,stdnames
         |,memberid
         |,regprovince
         |,regcity
         |,iscertified
         |,creationdate
         |,refreshmatflag
         |,mainproducttext
         |,isvip
         |,viptime
         |,certifiedtime
         |,fromtype
         |,registerip
         |,registeraddr
         |,source
         |,projectviewnum
         |,guideflag
         |,istest
         |,regcapitalunit
         |,mail
         |,upgradetips
         |,bid
         |,cast(regcapitalrmb as string) as  regcapitalrmb
         |,usertype
         |,levelcode
         |,projectviewprovince
         |,creditcode
         |,isenvironmental
         |,qq
         |,synthesisscore
         |,tmpstatus
         |FROM odm_db.o_tepshop
       """.stripMargin)
    tepshopToESDF.write.format("org.elasticsearch.spark.sql")
      .option("es.inex.auto.create","ture")
      .option("pushdown","true")
      .option("es.nodes","172.16.1.61")
      .option("es.port","9200")
      .option("es.nodes.wan.only","true")
      .mode(SaveMode.Overwrite)
      .save("test/o_tepshop")
    spark.stop()
  }

}
