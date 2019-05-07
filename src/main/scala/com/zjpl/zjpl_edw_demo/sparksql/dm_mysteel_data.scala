package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_mysteel_data {
    val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
    def main(args: Array[String]): Unit = {
      val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
      val spark = SparkSession
        .builder()
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
      import spark.sql
      sql(
        s"""
           |create table if not exists dm_db.dm_mysteel_data(
           |  name string comment '名称'
           | ,texture string comment '材质'
           | ,specification string comment '品牌'
           | ,city string comment '城市'
           | ,count string comment '数量'
           | ,warehouse string comment '仓库'
           | ,plant string comment '工厂'
           | ,supplier string comment '供应商'
           | ,contact string comment '联系人'
           | ,telephone string comment '电话号码'
           | ,phone bigint comment '手机号码'
           | ,time string comment '时间'
           | ,yest_price decimal(14,4) comment '昨天价格'
           | ,last_month_price decimal(14,4) comment '上月价格'
           | ,price decimal(14,4)  comment '价格'
           | ,day_ring_ratio_price  decimal(14,4) comment '日环比价格'
           | ,month_ring_ratio_price decimal(14,4) comment '月环比价格'
           | )partitioned by (etl_dt string)
           | stored as parquet
         """.stripMargin)
      var yest_dt: String = "1990-09-09"
      var dt_date01:String ="1990-09-09"
      var last_month_date:String ="1990-09-09"
      val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      if (args.length !=0)
      {
        yest_dt = args(0)
        val yest_date=dateForamt.parse(yest_dt)
        val cal_parse =Calendar.getInstance()
        val cal_parse01 =Calendar.getInstance()
        cal_parse.setTime(yest_date)
        cal_parse.add(Calendar.DATE,-1)
        cal_parse01.setTime(yest_date)
        cal_parse01.add(Calendar.MONTH,-1)
        dt_date01=dateForamt.format(cal_parse.getTime)
        last_month_date=dateForamt.format(cal_parse01.getTime)
      }else
      {
        val cal = Calendar.getInstance()
        cal.setTime(new Date())
        cal.add(Calendar.DATE,-1)
        yest_dt =dateForamt.format(cal.getTime)
        cal.add(Calendar.DATE,-1)
        dt_date01 =dateForamt.format(cal.getTime)
        val last_month = Calendar.getInstance()
        last_month.add(Calendar.DATE,-1)
        last_month.add(Calendar.MONTH,-1)
        last_month_date = dateForamt.format(last_month.getTime)
      }
      sql("alter table dm_db.dm_mysteel_data drop if exists partition (etl_dt ='"+yest_dt+"')")
      val exec_sql=sql(
        s"""
           |insert into table dm_db.dm_mysteel_data partition (etl_dt='$yest_dt')
           |select t1.name
           |      ,t1.texture
           |      ,t1.specification
           |      ,t1.city
           |      ,t1.count
           |      ,t1.warehouse
           |      ,t1.plant
           |      ,t1.supplier
           |      ,t1.contact
           |      ,t1.telephone
           |      ,t1.phone
           |      ,t1.time
           |      ,nvl(t2.price,0) as yest_price
           |      ,nvl(t3.price,0) as last_month_price
           |      ,nvl(t1.price,0) as price
           |      ,cast(case when nvl(t2.price,0) =0 and nvl(t1.price,0) !=0
           |                 then 1
           |                 when nvl(t2.price,0) =0 and nvl(t1.price,0) =0
           |                 then 0
           |                 when nvl(t2.price,0) !=0 and nvl(t1.price,0) =0
           |                 then -1
           |                 else (nvl(t1.price,0)-nvl(t2.price,0))/nvl(t2.price,0)
           |             end as decimal(14,4)) as day_ring_ratio_price
           |       ,cast(case when nvl(t3.price,0) =0 and t1.price !=0
           |                 then 1
           |                 when nvl(t3.price,0) =0 and t1.price =0
           |                 then 0
           |                 when nvl(t3.price,0) !=0 and t1.price =0
           |                 then -1
           |                 else (t1.price-nvl(t3.price,0))/nvl(t3.price,0)
           |             end as decimal(14,4)) as month_ring_ratio_price
           | from pdw_db.p_mysteel_data t1
           | left join pdw_db.p_mysteel_data t2
           |        on nvl(t1.name,' ') = nvl(t2.name,' ')
           |       and nvl(t1.texture,' ') = nvl(t2.texture,' ')
           |       and nvl(t1.specification,' ') = nvl(t2.specification,' ')
           |       and nvl(t1.city,' ') = nvl(t2.city,' ')
           |       and nvl(t1.plant,' ') = nvl(t2.plant,' ')
           |       and nvl(t1.supplier,' ') = nvl(t2.supplier,' ')
           |       and nvl(t1.warehouse,' ') = nvl(t2.warehouse,' ')
           |       and to_date(t2.etl_dt)= to_date('$dt_date01')
           | left join pdw_db.p_mysteel_data t3
           |        on nvl(t1.name,' ') = nvl(t3.name,' ')
           |       and nvl(t1.texture,' ') = nvl(t3.texture,' ')
           |       and nvl(t1.specification,' ') = nvl(t3.specification,' ')
           |       and nvl(t1.city,' ') = nvl(t3.city,' ')
           |       and nvl(t1.plant,' ') = nvl(t3.plant,' ')
           |       and nvl(t1.supplier,' ') = nvl(t3.supplier,' ')
           |       and nvl(t1.warehouse,' ') = nvl(t3.warehouse,' ')
           |       and to_date(t3.etl_dt)=to_date('$last_month_date')
           |where to_date(t1.etl_dt)= to_date('$yest_dt')
         """.stripMargin)
      logger.warn("exec_sql"+exec_sql)
      spark.stop()
  }
}

