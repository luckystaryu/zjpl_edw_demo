package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tepshop_h {
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
    sql(
      s"""
         |create table if not exists ods_db.o_tepshop_h(
         |  start_dt string,
         |  end_dt string,
         |  id int,
         |  eid string  comment '企业标识,审核后为系统分配,修改为可为空',
         |  name string  comment '企业名称',
         |  addr string comment '厂商地址',
         |  postcode string comment '邮政编码',
         |  homepage string comment '网站主页',
         |  corpn string comment '企业法人',
         |  area string comment '企业所在地区',
         |  logo string comment '企业logo图片',
         |  longlogo string,
         |  adurl string,
         |  culture string comment '企业文化',
         |  bglogo string comment '背景图片',
         |  linkman string comment '其他联系人的相关信息',
         |  mainmaterial string comment '主营材料',
         |  salearea string comment '销售区域',
         |  branchinfo string comment '品牌信息',
         |  provsort string comment '地区排序',
         |  province string comment '省份',
         |  city string comment '城市',
         |  picpath string comment '企业图片地址',
         |  sqspath string comment '授权书图片地址',
         |  brand string ,
         |  discription string comment '描述',
         |  regarea string comment '注册地',
         |  regaddress string comment '注册地址',
         |  regcapital decimal(14,2)  comment '注册资金',
         |  regtime timestamp  comment '注册时间',
         |  regoffice string comment '注册单位',
         |  starttime timestamp  comment '开始时间',
         |  endtime timestamp  comment '结束时间',
         |  regnum string comment '注册号',
         |  businessscope string comment '经营范围',
         |  businesslicensepic string comment '营业执照图片地址',
         |  businessmode tinyint  comment '1生产厂家2经销批发 3商业服务 4招商代理 5其他',
         |  enterprisetype string comment '企业类型',
         |  shoptype string ,
         |  createon timestamp ,
         |  createby string ,
         |  updateon timestamp ,
         |  updateby string ,
         |  islock tinyint ,
         |  isaudit tinyint ,
         |  isdeleted int  comment '删除(1-删除)',
         |  auditor string ,
         |  effectdate timestamp  comment '诚信联盟有效时间',
         |  auditdate timestamp  comment '最新审核时间',
         |  validdate timestamp ,
         |  isvolid tinyint ,
         |  manageid string ,
         |  degree int  ,
         |  cid string ,
         |  subcid string ,
         |  catalogid string ,
         |  grade string ,
         |  hasunauditmat int ,
         |  matcount int ,
         |  contact string ,
         |  sex string ,
         |  department string ,
         |  mobile string ,
         |  phone string ,
         |  isintegrity string ,
         |  creditscore int  comment '诚信积分',
         |  authcontent string comment '认证年限',
         |  recommendcontent string comment '采购商推荐',
         |  putcount int  comment '入库数',
         |  certifnum string comment '认证证书编号',
         |  fax string ,
         |  integritylogo string ,
         |  istop tinyint  ,
         |  refcount int  ,
         |  matupdateon timestamp  comment '供应商报价更新时间(指当前供应商下的报价全部已更新所对应的时间)',
         |  publishdate timestamp  comment '发布日期',
         |  isflagship string ,
         |  ishonourable string ,
         |  isjournal tinyint ,
         |  association string,
         |  stdnames string,
         |  memberid string  comment '用户账号',
         |  regprovince string comment '注册省份',
         |  regcity string comment '注册城市',
         |  iscertified tinyint   comment '是否认证(0:否，1：是，2：认证不通过)',
         |  creationdate string  comment '成立日期',
         |  refreshmatflag tinyint   comment '是否可以一键刷新，一天一次（1：是，0：否）',
         |  mainproducttext string comment '主营产品',
         |  isvip tinyint   comment '是否入库是否入库（0：待审核，1：审核通过，2：审核不通过）',
         |  viptime timestamp  comment '入库通过时间',
         |  certifiedtime timestamp  comment '认证通过时间',
         |  fromtype string  comment '来源',
         |  registerip string  comment '注册ip',
         |  registeraddr string  comment '注册ip',
         |  source tinyint   comment '1:造价通，2:gcw',
         |  projectviewnum int  comment '工程查看数量',
         |  guideflag int  comment '是否显示引导,0:引导，1：不引导',
         |  istest tinyint  comment '是否测试账号(0：否，1：是)',
         |  regcapitalunit string  comment '注册资金单位',
         |  mail string  comment '邮箱',
         |  upgradetips tinyint  comment '升级提示：1：提示，0：不提示',
         |  bid string comment '大类',
         |  regcapitalrmb decimal(14,2) comment '注册人民币，万元',
         |  usertype string   comment '用户类型,例如供应商：gys',
         |  levelcode string  comment '供应商等级',
         |  projectviewprovince string comment '工程信息查看省份',
         |  creditcode string  comment '社会统一信用代码',
         |  isenvironmental tinyint   comment '是否环保企业(0:不是，1：是)',
         |  qq string  comment '客服qq',
         |  synthesisscore int  comment '企业综合得分',
         |  tmpstatus int   comment '临时状态'
         |  ) stored as parquet
       """.stripMargin)
    /**
      * 1.将历史数据先保留到临时表中
      */
    val o_tepshop_h_tmp=sql(
      s"""
         |select *
         |from ods_db.o_tepshop_h
         |where to_date(start_dt) <to_date('$yest_dt')
       """.stripMargin)
    sql(
      s"""
         |drop table if exists ods_db.o_tepshop_h_tmp purge
       """.stripMargin)
    o_tepshop_h_tmp.write.mode(SaveMode.Overwrite).saveAsTable("ods_db.o_tepshop_h_tmp")
    /**
      * 2.将历史变动的数据全部置为失效
      */
    val o_tepshop_h_tmp01=sql(
      s"""
         |insert overwrite table ods_db.o_tepshop_h
         |select t1.start_dt
         |,'$yest_dt' as end_dt
         |,t1.id
         |,t1.eid
         |,t1.name
         |,t1.addr
         |,t1.postcode
         |,t1.homepage
         |,t1.corpn
         |,t1.area
         |,t1.logo
         |,t1.longlogo
         |,t1.adurl
         |,t1.culture
         |,t1.bglogo
         |,t1.linkman
         |,t1.mainmaterial
         |,t1.salearea
         |,t1.branchinfo
         |,t1.provsort
         |,t1.province
         |,t1.city
         |,t1.picpath
         |,t1.sqspath
         |,t1.brand
         |,t1.discription
         |,t1.regarea
         |,t1.regaddress
         |,t1.regcapital
         |,t1.regtime
         |,t1.regoffice
         |,t1.starttime
         |,t1.endtime
         |,t1.regnum
         |,t1.businessscope
         |,t1.businesslicensepic
         |,t1.businessmode
         |,t1.enterprisetype
         |,t1.shoptype
         |,t1.createon
         |,t1.createby
         |,t1.updateon
         |,t1.updateby
         |,t1.islock
         |,t1.isaudit
         |,t1.isdeleted
         |,t1.auditor
         |,t1.effectdate
         |,t1.auditdate
         |,t1.validdate
         |,t1.isvolid
         |,t1.manageid
         |,t1.degree
         |,t1.cid
         |,t1.subcid
         |,t1.catalogid
         |,t1.grade
         |,t1.hasunauditmat
         |,t1.matcount
         |,t1.contact
         |,t1.sex
         |,t1.department
         |,t1.mobile
         |,t1.phone
         |,t1.isintegrity
         |,t1.creditscore
         |,t1.authcontent
         |,t1.recommendcontent
         |,t1.putcount
         |,t1.certifnum
         |,t1.fax
         |,t1.integritylogo
         |,t1.istop
         |,t1.refcount
         |,t1.matupdateon
         |,t1.publishdate
         |,t1.isflagship
         |,t1.ishonourable
         |,t1.isjournal
         |,t1.association
         |,t1.stdnames
         |,t1.memberid
         |,t1.regprovince
         |,t1.regcity
         |,t1.iscertified
         |,t1.creationdate
         |,t1.refreshmatflag
         |,t1.mainproducttext
         |,t1.isvip
         |,t1.viptime
         |,t1.certifiedtime
         |,t1.fromtype
         |,t1.registerip
         |,t1.registeraddr
         |,t1.source
         |,t1.projectviewnum
         |,t1.guideflag
         |,t1.istest
         |,t1.regcapitalunit
         |,t1.mail
         |,t1.upgradetips
         |,t1.bid
         |,t1.regcapitalrmb
         |,t1.usertype
         |,t1.levelcode
         |,t1.projectviewprovince
         |,t1.creditcode
         |,t1.isenvironmental
         |,t1.qq
         |,t1.synthesisscore
         |,t1.tmpstatus
         |from ods_db.o_tepshop_h_tmp t1
         |left semi join ods_db.o_tepshop t2
         |  on t1.id = t2.id
       """.stripMargin)
    /**
      *3.新增的数据
      */
    val o_tepshop_h_tmp02=sql(
      s"""
         |insert into table ods_db.o_tepshop_h
         |select '$yest_dt' as start_dt
         |,'2999-12-31' as end_dt
         |,t1.id
         |,t1.eid
         |,t1.name
         |,t1.addr
         |,t1.postcode
         |,t1.homepage
         |,t1.corpn
         |,t1.area
         |,t1.logo
         |,t1.longlogo
         |,t1.adurl
         |,t1.culture
         |,t1.bglogo
         |,t1.linkman
         |,t1.mainmaterial
         |,t1.salearea
         |,t1.branchinfo
         |,t1.provsort
         |,t1.province
         |,t1.city
         |,t1.picpath
         |,t1.sqspath
         |,t1.brand
         |,t1.discription
         |,t1.regarea
         |,t1.regaddress
         |,t1.regcapital
         |,t1.regtime
         |,t1.regoffice
         |,t1.starttime
         |,t1.endtime
         |,t1.regnum
         |,t1.businessscope
         |,t1.businesslicensepic
         |,t1.businessmode
         |,t1.enterprisetype
         |,t1.shoptype
         |,t1.createon
         |,t1.createby
         |,t1.updateon
         |,t1.updateby
         |,t1.islock
         |,t1.isaudit
         |,t1.isdeleted
         |,t1.auditor
         |,t1.effectdate
         |,t1.auditdate
         |,t1.validdate
         |,t1.isvolid
         |,t1.manageid
         |,t1.degree
         |,t1.cid
         |,t1.subcid
         |,t1.catalogid
         |,t1.grade
         |,t1.hasunauditmat
         |,t1.matcount
         |,t1.contact
         |,t1.sex
         |,t1.department
         |,t1.mobile
         |,t1.phone
         |,t1.isintegrity
         |,t1.creditscore
         |,t1.authcontent
         |,t1.recommendcontent
         |,t1.putcount
         |,t1.certifnum
         |,t1.fax
         |,t1.integritylogo
         |,t1.istop
         |,t1.refcount
         |,t1.matupdateon
         |,t1.publishdate
         |,t1.isflagship
         |,t1.ishonourable
         |,t1.isjournal
         |,t1.association
         |,t1.stdnames
         |,t1.memberid
         |,t1.regprovince
         |,t1.regcity
         |,t1.iscertified
         |,t1.creationdate
         |,t1.refreshmatflag
         |,t1.mainproducttext
         |,t1.isvip
         |,t1.viptime
         |,t1.certifiedtime
         |,t1.fromtype
         |,t1.registerip
         |,t1.registeraddr
         |,t1.source
         |,t1.projectviewnum
         |,t1.guideflag
         |,t1.istest
         |,t1.regcapitalunit
         |,t1.mail
         |,t1.upgradetips
         |,t1.bid
         |,t1.regcapitalrmb
         |,t1.usertype
         |,t1.levelcode
         |,t1.projectviewprovince
         |,t1.creditcode
         |,t1.isenvironmental
         |,t1.qq
         |,t1.synthesisscore
         |,t1.tmpstatus
         |from ods_db.o_tepshop t1
       """.stripMargin)
    val o_tepshop_h_tmp03=sql(
      s"""
         |insert into table ods_db.o_tepshop_h
         |select t1.start_dt
         |,t1.end_dt
         |,t1.id
         |,t1.eid
         |,t1.name
         |,t1.addr
         |,t1.postcode
         |,t1.homepage
         |,t1.corpn
         |,t1.area
         |,t1.logo
         |,t1.longlogo
         |,t1.adurl
         |,t1.culture
         |,t1.bglogo
         |,t1.linkman
         |,t1.mainmaterial
         |,t1.salearea
         |,t1.branchinfo
         |,t1.provsort
         |,t1.province
         |,t1.city
         |,t1.picpath
         |,t1.sqspath
         |,t1.brand
         |,t1.discription
         |,t1.regarea
         |,t1.regaddress
         |,t1.regcapital
         |,t1.regtime
         |,t1.regoffice
         |,t1.starttime
         |,t1.endtime
         |,t1.regnum
         |,t1.businessscope
         |,t1.businesslicensepic
         |,t1.businessmode
         |,t1.enterprisetype
         |,t1.shoptype
         |,t1.createon
         |,t1.createby
         |,t1.updateon
         |,t1.updateby
         |,t1.islock
         |,t1.isaudit
         |,t1.isdeleted
         |,t1.auditor
         |,t1.effectdate
         |,t1.auditdate
         |,t1.validdate
         |,t1.isvolid
         |,t1.manageid
         |,t1.degree
         |,t1.cid
         |,t1.subcid
         |,t1.catalogid
         |,t1.grade
         |,t1.hasunauditmat
         |,t1.matcount
         |,t1.contact
         |,t1.sex
         |,t1.department
         |,t1.mobile
         |,t1.phone
         |,t1.isintegrity
         |,t1.creditscore
         |,t1.authcontent
         |,t1.recommendcontent
         |,t1.putcount
         |,t1.certifnum
         |,t1.fax
         |,t1.integritylogo
         |,t1.istop
         |,t1.refcount
         |,t1.matupdateon
         |,t1.publishdate
         |,t1.isflagship
         |,t1.ishonourable
         |,t1.isjournal
         |,t1.association
         |,t1.stdnames
         |,t1.memberid
         |,t1.regprovince
         |,t1.regcity
         |,t1.iscertified
         |,t1.creationdate
         |,t1.refreshmatflag
         |,t1.mainproducttext
         |,t1.isvip
         |,t1.viptime
         |,t1.certifiedtime
         |,t1.fromtype
         |,t1.registerip
         |,t1.registeraddr
         |,t1.source
         |,t1.projectviewnum
         |,t1.guideflag
         |,t1.istest
         |,t1.regcapitalunit
         |,t1.mail
         |,t1.upgradetips
         |,t1.bid
         |,t1.regcapitalrmb
         |,t1.usertype
         |,t1.levelcode
         |,t1.projectviewprovince
         |,t1.creditcode
         |,t1.isenvironmental
         |,t1.qq
         |,t1.synthesisscore
         |,t1.tmpstatus
         | from ods_db.o_tepshop_h_tmp t1
         | left join ods_db.o_tepshop t2
         |  on t1.id = t2.id
         | where t2.id is null
       """.stripMargin)
    sql(
      s"""
         |drop table if exists ods_db.o_tepshop_h_tmp purge
       """.stripMargin)
    spark.stop()
  }
}
