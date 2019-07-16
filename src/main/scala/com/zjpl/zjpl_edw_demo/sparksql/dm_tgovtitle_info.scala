package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_tgovtitle_info {
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
         |create table if not exists dm_db.dm_tgovtitle_info
         |( etl_dt string comment '统计日期'
         | ,issue_year string comment '发布年份'
         | ,webprovince string
         | ,webArea     string
         | ,webcounty   string
         | ,jan_issue_flag int comment '一月份发布标识'
         | ,feb_issue_flag int comment '二月发布标识'
         | ,march_issue_flag int comment '三月发布标识'
         | ,april_issue_flag int comment '四月发布标识'
         | ,may_issue_flag int comment '五月发布标识'
         | ,june_issue_flag int comment '六月发布标识'
         | ,july_issue_flag int comment '七月发布标识'
         | ,aug_issue_flag  int comment '八月发布标识'
         | ,sept_issue_flag int comment '九月发布标识'
         | ,oct_issue_flag int comment '十月发布标识'
         | ,nov_issue_flag int comment '十一月发布标识'
         | ,dec_issue_flag int comment '十二月发布标识'
         | ,first_quar_issue_flag int comment '第一季度'
         | ,second_quar_issue_flag int comment '第二季度'
         | ,third_quar_issue_flag int comment '第三季度'
         | ,fourth_quar_issue_falg int comment '第四季度'
         |)stored as parquet
       """.stripMargin)
    sql(
      s"""
         |insert overwrite table dm_db.dm_tgovtitle_info
         |select '$yest_dt'
         |      ,cast(year(t2.issuedate) as string) as issue_year
         |      ,t1.name_1                as webprovince
         |      ,t1.name_2                as webArea
         |      ,t1.name                  as webcounty
         |      ,max(case when month(t2.issuedate) =1 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |            end) as jan_issue_flag
         |      ,max(case when month(t2.issuedate) =2 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as feb_issue_flag
         |      ,max(case when month(t2.issuedate) =3 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as march_issue_flag
         |      ,max(case when month(t2.issuedate) =4 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as april_issue_flag
         |      ,max(case when month(t2.issuedate) =5 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as may_issue_flag
         |      ,max(case when month(t2.issuedate) =6 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as june_issue_flag
         |      ,max(case when month(t2.issuedate) =7 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as july_issue_flag
         |      ,max(case when month(t2.issuedate) =8 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as aug_issue_flag
         |      ,max(case when month(t2.issuedate) =9 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as sept_issue_flag
         |      ,max(case when month(t2.issuedate) =10 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as oct_issue_flag
         |      ,max(case when month(t2.issuedate) =11 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as nov_issue_flag
         |      ,max(case when month(t2.issuedate) =12 and day(t2.issuedate)=5
         |                then 1
         |                else 0
         |             end) as dec_issue_flag
         |      ,max(case when month(t2.issuedate) in (1,2,3) and day(t2.issuedate)=15
         |                then 1
         |                else 0
         |             end) as first_quar_issue_flag
         |      ,max(case when month(t2.issuedate) in (4,5,6) and day(t2.issuedate)=15
         |                then 1
         |                else 0
         |             end) as second_quar_issue_flag
         |      ,max(case when month(t2.issuedate) in (7,8,9) and day(t2.issuedate)=15
         |                then 1
         |                else 0
         |             end) as third_quar_issue_flag
         |      ,max(case when month(t2.issuedate) in (10,11,12) and day(t2.issuedate)=15
         |                then 1
         |                else 0
         |             end) as fourth_quar_issue_falg
         |  from pdw_db.p_tareamanagement t1
         |  left join pdw_db.p_tgovtitle t2
         |   on t1.name = t2.webcounty
         |  where t1.isGov=1
         |    and length(t1.regioncode) in (2,6)
         |  group by cast(year(t2.issuedate) as string)
         |      ,t1.name_1
         |      ,t1.name_2
         |      ,t1.name
       """.stripMargin)
  }
}
