package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

object o_tmember_h {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields",200)
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |Create table if not exists ods_db.o_tmember_h(
         |start_time               timestamp
         |,end_time                timestamp
         |,id                       int
         |,memberid                string
         |,webarea                 string
         |,pwd                     string
         |,truename                string
         |,sex                     string
         |,degree                  int
         |,postcode                string
         |,addr                    string
         |,email                   string
         |,qq                      string
         |,msn                     string
         |,phone                   string
         |,fax                     string
         |,mobile                  string
         |,lasttime                timestamp
         |,loginnum                int
         |,ipaddr                  string
         |,validdate               timestamp
         |,isvoid                  string
         |,islock                  int
         |,createon                timestamp
         |,updateon                timestamp
         |,updateby                string
         |,memo                    string
         |,corpname                string
         |,fid                     string
         |,regtype                 int
         |,membertype              string
         |,website                 string
         |,department              string
         |,logo                    string
         |,searchtimes             int
         |,eid                     string
         |,isadmin                 int
         |,lastdegree              int
         |,eplock                  int
         |,regprovince             string
         |,webprovince             string
         |,webcounty               string
         |,accesssite              string
         |,score                   int
         |,province                string
         |,city                    string
         |,asktotal                int
         |,audittime               timestamp
         |,ordersitem              string
         |,ordersitemtype          string
         |,status                  int
         |,inquirynum              int
         |,usescore                int
         |,signincount             int
         |,currscore               int
         |,exp                     int
         |,goodat                  string
         |,nickname                string
         |,frozenscore             int
         |,role                    int
         |,accountbalance          string
         |,defaultbank             string
         |,invoiceinfo             string
         |,major                   string
         |,purchasescore           int
         |,materialcount           int
         |,faccount                int
         |,curmaterialcount        string
         |,curfaccount             string
         |,qkvaliddate             timestamp
         |,lockedtime              timestamp
         |,wenkusignincount        int
         |,empstatus               boolean
         |,introduction            string
         |,resume                  string
         |,phoneverification       int
         |,matdownloadcount        int
         |,residuematdownloadcount int
         |,authorizationstatus     int
         |,capacityofcount         int
         |,cloudinterestedbuyers   int
         |,applyforon              timestamp
         |,enterprisecreationon    timestamp
         |,position                string
         |,qqopenid                string
         |,accesstoken             string
         |,regchannel              int
         |,qqstatus                int
         |,authorizationvip        int
         |,askcount                int
         |,residueaskcount         int
         |,totalaskcount           int
         |,govexcelcount           int
         |,supplierid              string
         |,assignaskcount          int
         |,assigngovexcelcount     int
         |,companytype             string
         |,weixinunionid           string
         |,weixinstatus            string
         |,islayer                 string
         |,mastermemberid          string
         |,age                     int
         |,education               int
         |,jobtitle                string
         |,attentioncontent        string
         |,regsource               string
         |)stored as parquet
       """.stripMargin)
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
    val o_tmember_h_tmp_tab=sql(
      s"""
         |create table if not exists ods_db.o_tmember_h_tmp as
         |select * from ods_db.o_tmember_h t1
         |where t1.start_time <cast('$yest_dt 00:00:00.0' as timestamp)
       """.stripMargin)
    val o_tmember_h_tmp_sql =sql(
      s"""
         |select
         | cast('$yest_dt 00:00:00.0' as timestamp) as start_time
         |,cast('2999-12-31 00:00:00.0' as timestamp) as end_time
         |,id
         |,memberid
         |,webarea
         |,pwd
         |,truename
         |,sex
         |,degree
         |,postcode
         |,addr
         |,email
         |,qq
         |,msn
         |,phone
         |,fax
         |,mobile
         |,lasttime
         |,loginnum
         |,ipaddr
         |,validdate
         |,isvoid
         |,islock
         |,createon
         |,updateon
         |,updateby
         |,memo
         |,corpname
         |,fid
         |,regtype
         |,membertype
         |,website
         |,department
         |,logo
         |,searchtimes
         |,eid
         |,isadmin
         |,lastdegree
         |,eplock
         |,regprovince
         |,webprovince
         |,webcounty
         |,accesssite
         |,score
         |,province
         |,city
         |,asktotal
         |,audittime
         |,ordersitem
         |,ordersitemtype
         |,status
         |,inquirynum
         |,usescore
         |,signincount
         |,currscore
         |,exp
         |,goodat
         |,nickname
         |,frozenscore
         |,role
         |,accountbalance
         |,defaultbank
         |,invoiceinfo
         |,major
         |,purchasescore
         |,materialcount
         |,faccount
         |,curmaterialcount
         |,curfaccount
         |,qkvaliddate
         |,lockedtime
         |,wenkusignincount
         |,empstatus
         |,introduction
         |,resume
         |,phoneverification
         |,matdownloadcount
         |,residuematdownloadcount
         |,authorizationstatus
         |,capacityofcount
         |,cloudinterestedbuyers
         |,applyforon
         |,enterprisecreationon
         |,position
         |,qqopenid
         |,accesstoken
         |,regchannel
         |,qqstatus
         |,authorizationvip
         |,askcount
         |,residueaskcount
         |,totalaskcount
         |,govexcelcount
         |,supplierid
         |,assignaskcount
         |,assigngovexcelcount
         |,companytype
         |,weixinunionid
         |,weixinstatus
         |,islayer
         |,mastermemberid
         |,age
         |,education
         |,jobtitle
         |,attentioncontent
         |,regsource
         | from ods_db.o_tmember
       """.stripMargin )
    val o_tmember_h_dis_tmp_sql =sql(
      s"""
         |select
         | t1.start_time
         |,cast('$yest_dt 00:00:00.0' as timestamp) as end_time
         |,t1.id
         |,t1.memberid
         |,t1.webarea
         |,t1.pwd
         |,t1.truename
         |,t1.sex
         |,t1.degree
         |,t1.postcode
         |,t1.addr
         |,t1.email
         |,t1.qq
         |,t1.msn
         |,t1.phone
         |,t1.fax
         |,t1.mobile
         |,t1.lasttime
         |,t1.loginnum
         |,t1.ipaddr
         |,t1.validdate
         |,t1.isvoid
         |,t1.islock
         |,t1.createon
         |,t1.updateon
         |,t1.updateby
         |,t1.memo
         |,t1.corpname
         |,t1.fid
         |,t1.regtype
         |,t1.membertype
         |,t1.website
         |,t1.department
         |,t1.logo
         |,t1.searchtimes
         |,t1.eid
         |,t1.isadmin
         |,t1.lastdegree
         |,t1.eplock
         |,t1.regprovince
         |,t1.webprovince
         |,t1.webcounty
         |,t1.accesssite
         |,t1.score
         |,t1.province
         |,t1.city
         |,t1.asktotal
         |,t1.audittime
         |,t1.ordersitem
         |,t1.ordersitemtype
         |,t1.status
         |,t1.inquirynum
         |,t1.usescore
         |,t1.signincount
         |,t1.currscore
         |,t1.exp
         |,t1.goodat
         |,t1.nickname
         |,t1.frozenscore
         |,t1.role
         |,t1.accountbalance
         |,t1.defaultbank
         |,t1.invoiceinfo
         |,t1.major
         |,t1.purchasescore
         |,t1.materialcount
         |,t1.faccount
         |,t1.curmaterialcount
         |,t1.curfaccount
         |,t1.qkvaliddate
         |,t1.lockedtime
         |,t1.wenkusignincount
         |,t1.empstatus
         |,t1.introduction
         |,t1.resume
         |,t1.phoneverification
         |,t1.matdownloadcount
         |,t1.residuematdownloadcount
         |,t1.authorizationstatus
         |,t1.capacityofcount
         |,t1.cloudinterestedbuyers
         |,t1.applyforon
         |,t1.enterprisecreationon
         |,t1.position
         |,t1.qqopenid
         |,t1.accesstoken
         |,t1.regchannel
         |,t1.qqstatus
         |,t1.authorizationvip
         |,t1.askcount
         |,t1.residueaskcount
         |,t1.totalaskcount
         |,t1.govexcelcount
         |,t1.supplierid
         |,t1.assignaskcount
         |,t1.assigngovexcelcount
         |,t1.companytype
         |,t1.weixinunionid
         |,t1.weixinstatus
         |,t1.islayer
         |,t1.mastermemberid
         |,t1.age
         |,t1.education
         |,t1.jobtitle
         |,t1.attentioncontent
         |,t1.regsource
         | from ods_db.o_tmember_h_tmp t1
         | left semi join ods_db.o_tmember t2
         |  on t1.id = t2.id
       """.stripMargin)
    val o_tmember_h_eff_tmp_sql =sql(
      s"""
         |select
         | t1.start_time
         |,t1.end_time
         |,t1.id
         |,t1.memberid
         |,t1.webarea
         |,t1.pwd
         |,t1.truename
         |,t1.sex
         |,t1.degree
         |,t1.postcode
         |,t1.addr
         |,t1.email
         |,t1.qq
         |,t1.msn
         |,t1.phone
         |,t1.fax
         |,t1.mobile
         |,t1.lasttime
         |,t1.loginnum
         |,t1.ipaddr
         |,t1.validdate
         |,t1.isvoid
         |,t1.islock
         |,t1.createon
         |,t1.updateon
         |,t1.updateby
         |,t1.memo
         |,t1.corpname
         |,t1.fid
         |,t1.regtype
         |,t1.membertype
         |,t1.website
         |,t1.department
         |,t1.logo
         |,t1.searchtimes
         |,t1.eid
         |,t1.isadmin
         |,t1.lastdegree
         |,t1.eplock
         |,t1.regprovince
         |,t1.webprovince
         |,t1.webcounty
         |,t1.accesssite
         |,t1.score
         |,t1.province
         |,t1.city
         |,t1.asktotal
         |,t1.audittime
         |,t1.ordersitem
         |,t1.ordersitemtype
         |,t1.status
         |,t1.inquirynum
         |,t1.usescore
         |,t1.signincount
         |,t1.currscore
         |,t1.exp
         |,t1.goodat
         |,t1.nickname
         |,t1.frozenscore
         |,t1.role
         |,t1.accountbalance
         |,t1.defaultbank
         |,t1.invoiceinfo
         |,t1.major
         |,t1.purchasescore
         |,t1.materialcount
         |,t1.faccount
         |,t1.curmaterialcount
         |,t1.curfaccount
         |,t1.qkvaliddate
         |,t1.lockedtime
         |,t1.wenkusignincount
         |,t1.empstatus
         |,t1.introduction
         |,t1.resume
         |,t1.phoneverification
         |,t1.matdownloadcount
         |,t1.residuematdownloadcount
         |,t1.authorizationstatus
         |,t1.capacityofcount
         |,t1.cloudinterestedbuyers
         |,t1.applyforon
         |,t1.enterprisecreationon
         |,t1.position
         |,t1.qqopenid
         |,t1.accesstoken
         |,t1.regchannel
         |,t1.qqstatus
         |,t1.authorizationvip
         |,t1.askcount
         |,t1.residueaskcount
         |,t1.totalaskcount
         |,t1.govexcelcount
         |,t1.supplierid
         |,t1.assignaskcount
         |,t1.assigngovexcelcount
         |,t1.companytype
         |,t1.weixinunionid
         |,t1.weixinstatus
         |,t1.islayer
         |,t1.mastermemberid
         |,t1.age
         |,t1.education
         |,t1.jobtitle
         |,t1.attentioncontent
         |,t1.regsource
         | from ods_db.o_tmember_h_tmp t1
         | left join ods_db.o_tmember t2
         |  on t1.id = t2.id
         | where t2.id is not null
       """.stripMargin)
       o_tmember_h_tmp_sql.unionAll(o_tmember_h_dis_tmp_sql).unionAll(o_tmember_h_eff_tmp_sql)
      .write.mode(SaveMode.Overwrite).saveAsTable("ods_db.o_tmember_h")
    sql(
      s"""
         |drop table if exists ods_db.o_tmember_h_tmp
       """.stripMargin)
    spark.stop()
  }
}
