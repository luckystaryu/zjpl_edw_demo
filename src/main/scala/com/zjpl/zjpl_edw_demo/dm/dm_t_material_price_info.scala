package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_t_material_price_info {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",800)
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
         | create table if not exists dm_db.dm_t_material_price_info
         | (
         |  issuedate timestamp comment '发布日期'
         | ,code10 string comment '代码'
         | ,addr   string comment '地址'
         | ,name   string comment '名称'
         | ,spec   string comment '规格'
         | ,unit   string comment '单位'
         | ,brand  string comment '品牌'
         | ,tax_price decimal(14,4) comment '含税价'
         | ,last_month_tax_price decimal(14,4) comment '上月含税价格'
         | ,last_year_tax_price  decimal(14,4) comment '去年同期含税价格'
         | ,no_tax_price decimal(14,4) comment '除税价'
         | ,last_month_no_tax_price decimal(14,4) comment '上月除税价格'
         | ,last_year_no_tax_price decimal(14,4) comment '去年同期含税价格'
         | ,tax_price_chain_rate decimal(5,4) comment '含税价环比'
         | ,tax_price_same_rate decimal(5,4) comment '含税同比'
         | ,no_tax_price_chain_rate decimal(5,4) comment '除税价环比'
         | ,no_tax_price_same_rate  decimal(5,4) comment '除税价同比'
         | )partitioned by(etl_dt string)
         | stored as parquet
       """.stripMargin)
    val dm_t_material_price_info_tmpDF=sql(
      s"""
         |select issuedate
         |,code10
         |,addr
         |,name
         |,spec
         |,unit
         |,brand
         |,avg(nvl(tax_price,0)) as tax_price
         |,avg(nvl(no_tax_price,0)) as no_tax_price
         | from pdw_db.p_t_material_price_info_zjt
         | group by issuedate
         |,code10
         |,addr
         |,name
         |,spec
         |,unit
         |,brand
         |""".stripMargin)
    dm_t_material_price_info_tmpDF.createOrReplaceGlobalTempView("dm_t_material_price_info_tmp")
    sql("alter table dm_db.dm_t_material_price_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table dm_db.dm_t_material_price_info drop if exists partition (etl_dt='"+yest_dt+"')")
      sql(
      s"""
         |insert into dm_db.dm_t_material_price_info partition (etl_dt='$yest_dt')
         |select t1.issuedate
         |,t1.code10
         |,t1.addr
         |,t1.name
         |,t1.spec
         |,t1.unit
         |,t1.brand
         |,nvl(t1.tax_price,0) as tax_price
         |,nvl(t2.tax_price,0) as last_month_tax_price
         |,nvl(t3.tax_price,0) as last_year_tax_price
         |,nvl(t1.no_tax_price,0) as no_tax_price
         |,nvl(t2.no_tax_price,0) as last_month_no_tax_price
         |,nvl(t3.no_tax_price,0) as last_year_no_tax_price
         |,cast(case when nvl(t1.tax_price,0)=0
         |            and nvl(t2.tax_price,0)!=0
         |           then -1
         |           when nvl(t1.tax_price,0)=0
         |            and nvl(t2.tax_price,0)=0
         |           then 0
         |           else (nvl(t1.tax_price,0)-nvl(t2.tax_price,0))/nvl(t1.tax_price,0)
         |        end as decimal(5,4)) as tax_price_chain_rate
         |,cast(case when nvl(t1.tax_price,0)=0
         |            and nvl(t3.tax_price,0)!=0
         |           then -1
         |           when nvl(t1.tax_price,0)=0
         |            and nvl(t3.tax_price,0)=0
         |           then 0
         |            else (nvl(t1.tax_price,0)-nvl(t3.tax_price,0))/nvl(t1.tax_price,0)
         |        end as decimal(5,4)) as tax_price_same_rate
         |,cast(case when nvl(t1.no_tax_price,0)=0
         |            and nvl(t2.no_tax_price,0)!=0
         |           then -1
         |           when nvl(t1.no_tax_price,0)=0
         |            and nvl(t2.no_tax_price,0)=0
         |           then 0
         |           else (nvl(t1.no_tax_price,0)-nvl(t2.no_tax_price,0))/nvl(t1.no_tax_price,0)
         |       end as decimal(5,4)) as no_tax_price_chain_rate
         | ,cast(case when nvl(t1.no_tax_price,0)=0
         |             and nvl(t3.no_tax_price,0)!=0
         |            then -1
         |            when nvl(t1.no_tax_price,0)=0
         |             and nvl(t3.no_tax_price,0)=0
         |            then 0
         |            else (nvl(t1.no_tax_price,0)-nvl(t3.no_tax_price,0))/nvl(t1.no_tax_price,0)
         |        end as decimal(5,4)) as no_tax_price_same_rate
         |  from global_temp.dm_t_material_price_info_tmp t1
         |  inner join global_temp.dm_t_material_price_info_tmp t2
         |    on to_date(t1.issuedate) =add_months(to_date(t2.issuedate),-1)
         |   and t1.code10 = t2.code10
         |   and t1.addr = t2.addr
         |   and t1.name = t2.name
         |   and t1.spec = t2.spec
         |   and t1.unit = t2.unit
         |   and t1.brand = t2.brand
         |  left  join global_temp.dm_t_material_price_info_tmp t3
         |    on to_date(t1.issuedate) =add_months(to_date(t3.issuedate),-12)
         |   and t1.code10 = t3.code10
         |   and t1.addr = t3.addr
         |   and t1.name = t3.name
         |   and t1.spec = t3.spec
         |   and t1.unit = t3.unit
         |   and t1.brand = t3.brand
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
