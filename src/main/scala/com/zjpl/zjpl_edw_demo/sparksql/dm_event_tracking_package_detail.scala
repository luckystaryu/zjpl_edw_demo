package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_event_tracking_package_detail {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |create table if not exists dm_db.dm_event_tracking_package_detail
         |(
         |  webId string comment '页面ID',
         |  type  string comment '事件类型1：进入,2:点击,3 离开',
         |  labelName string comment '打标标识',
         |  referer  string comment '相关页',
         |  screenHeight string comment '屏幕高度',
         |  screenWidth  string comment '屏幕宽度',
         |  screenColorDepth string comment '屏幕',
         |  screenAvailHeight  string,
         |  screenAvailWidth string,
         |  title  string,
         |  domain string,
         |  url  string,
         |  browserLang string,
         |  browseAgent string,
         |  browser string,
         |  cookieEnabled string,
         |  system string,
         |  systemVersion string,
         |  memberId string,
         |  trueName string comment '真实姓名',
         |  nickname string comment '昵称',
         |  province string comment '省',
         |  city     string comment '市',
         |  sex      string comment '性别',
         |  major    string comment '专业',
         |  MemberType string comment '会员类型',
         |  CorpName   string comment '企业名称',
         |  companyType string comment '单位性质',
         |  mobile      string comment '手机号码',
         |  phone       string comment '联系电话',
         |  QQ          string comment 'QQ',
         |  Email       string comment '邮箱',
         |  attentionContent string comment '关注内容',
         |  sessionId string,
         |  ip string,
         |  createTime string,
         |  stopTime  string,
         |  source_type_cd int
         |)partitioned by (etl_dt string)
         |stored as parquet
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
    sql("alter table dm_db.dm_event_tracking_package_detail drop if exists partition(etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table dm_db.dm_event_tracking_package_detail partition (etl_dt='$yest_dt')
         |select  t1.webId
         |,t1.type
         |,t1.labelName
         |,t1.referer
         |,t1.screenHeight
         |,t1.screenWidth
         |,t1.screenColorDepth
         |,t1.screenAvailHeight
         |,t1.screenAvailWidth
         |,t1.title
         |,t1.domain
         |,t1.url
         |,t1.browserLang
         |,t1.browseAgent
         |,t1.browser
         |,t1.cookieEnabled
         |,t1.system
         |,t1.systemVersion
         |,nvl(t2.memberId,t1.username) as memberId
         |,t2.trueName
         |,t2.nickname
         |,t2.province
         |,t2.city
         |,t2.sex
         |,t2.major
         |,t2.MemberType
         |,t2.CorpName
         |,t2.companyType
         |,t2.mobile
         |,t2.phone
         |,t2.QQ
         |,t2.Email
         |,t2.attentionContent
         |,t1.sessionId
         |,t1.ip
         |,t1.createTime
         |,t1.stopTime
         |,1 as source_type_cd
         |  from ods_db.o_stg_small_h t1
         |  left join ods_db.o_tmember_h t2
         |    on t1.userName = t2.memberId
         |   and t2.start_time <= cast('$yest_dt 00:00:00.0' as timestamp)
         |   and t2.end_time > cast('$yest_dt 00:00:00.0' as timestamp)
         | where to_date(t1.etl_dt) = to_date('$yest_dt')
         |   and t1.ip not in ('14.23.112.114','14.23.112.115','14.23.112.116','14.23.112.117','14.23.112.118')
         |   and (t1.webid like '%-pc'
         |      or t1.webid like '%-buy-100'
         |      or t1.webid like '%-buy-down'
         |      or t1.webid like '%-buy-submit'
         |      or t1.webid like '%-buy-shoppingcart1'
         |      or t1.webid like '%-buy-up'
         |      or t1.webid like '%-buy-search')
       """.stripMargin)
    sql(
      s"""
         |insert into table dm_db.dm_event_tracking_package_detail partition (etl_dt='$yest_dt')
         |select  t1.webId
         |,t1.type
         |,t1.labelName
         |,t1.referer
         |,t1.screenHeight
         |,t1.screenWidth
         |,t1.screenColorDepth
         |,t1.screenAvailHeight
         |,t1.screenAvailWidth
         |,t1.title
         |,t1.domain
         |,t1.url
         |,t1.browserLang
         |,t1.browseAgent
         |,t1.browser
         |,t1.cookieEnabled
         |,t1.system
         |,t1.systemVersion
         |,nvl(t2.memberId,t1.username) as memberId
         |,t2.trueName
         |,t2.nickname
         |,t2.province
         |,t2.city
         |,t2.sex
         |,t2.major
         |,t2.MemberType
         |,t2.CorpName
         |,t2.companyType
         |,t2.mobile
         |,t2.phone
         |,t2.QQ
         |,t2.Email
         |,t2.attentionContent
         |,t1.sessionId
         |,t1.ip
         |,t1.createTime
         |,t1.stopTime
         |,2 as source_type_cd
         | from ods_db.o_stg_mobile_h t1
         | left join ods_db.o_tmember_h t2
         |   on t1.userName = t2.memberId
         |  and t2.start_time <= cast('$yest_dt 00:00:00.0' as timestamp)
         |  and t2.end_time > cast('$yest_dt 00:00:00.0' as timestamp)
         |where to_date(t1.etl_dt) = to_date('$yest_dt')
         |  and t1.ip not in ('14.23.112.114','14.23.112.115','14.23.112.116','14.23.112.117','14.23.112.118')
         |  and (t1.webid like '%-m'
         |      or t1.webid like 'mobile-%')
       """.stripMargin)
  }
}
