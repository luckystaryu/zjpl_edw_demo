package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_fac_material_price_any {
 val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
    def main(args: Array[String]): Unit = {
      val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
      val spark = SparkSession
        .builder()
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .config("spark.debug.maxToStringFields",200)
        .enableHiveSupport()
        .getOrCreate()
      import spark.sql
      sql(
        s"""
           |create table if not exists dm_db.dm_fac_material_price_any
           |(
           |   cid     string comment '一级分类id',
           |   subcid  string comment '二级分类id',
           |   stdname string comment '标准名称',
           |   features string comment '特征',
           |   unit     string comment '单位',
           |   max_pricem decimal(14,4) comment '最大价格',
           |   min_pricem decimal(14,4) comment '最小价格',
           |   mean_pricem decimal(14,4) comment '均值',
           |   stddev_pricem decimal(14,4) comment '标准差',
           |   variance_pricem decimal(14,4) comment '方差',
           |   first_max_pricem decimal(14,4)  comment '第一个系数最大值(0.9)',
           |   first_min_pricem decimal(14,4) comment '第一个系数最小值(0.9)',
           |   second_max_pricem decimal(14,4) comment '第二个系数最大值(0.8)',
           |   second_min_pricem decimal(14,4) comment '第二个系数最小值(0.8)',
           |   first_diff_max_pricem decimal(14,4) comment '第一个系数最大值和第二个系数最大值差值',
           |   first_diff_min_pricem decimal(14,4) comment '第一个系数最小值和第二个系数最小值差值',
           |   third_max_pricem decimal(14,4) comment '第三个系数最大值(0.7)',
           |   third_min_pricem decimal(14,4) comment '第三个系数最大值(0.7)',
           |   second_diff_max_pricem decimal(14,4) comment '第二个系数最大值和第三个系数最大值差值',
           |   second_diff_min_pricem decimal(14,4) comment '第二个系数最大值和第三个系数最小值差值',
           |   data_cnt bigint comment '数据量'
            |)partitioned by (etl_dt string)
           |stored as parquet
         """.stripMargin)
      var yest_dt: String = "1990-09-09"
      var dt_date01:String ="1990-09-09"
      var last_month_date:String ="1990-09-09"
      val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      var v_coefficient:Double=0
      var v_coefficient01:Double =0
      var v_coefficient02:Double =0
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
        v_coefficient =0.9
        v_coefficient01 =0.8
        v_coefficient02 =0.7
      }
      sql("alter table dm_db.dm_fac_material_price_any drop if exists partition (etl_dt ='"+yest_dt+"')")
      val exec_sql=sql(
        s"""
           |insert into table dm_db.dm_fac_material_price_any partition (etl_dt='$yest_dt')
           |select tt.cid
           |      ,tt.subcid
           |      ,tt.stdname
           |      ,tt.features
           |      ,tt.unit
           |      ,tt.max_pricem
           |      ,tt.min_pricem
           |      ,tt.mean_pricem
           |      ,tt.stddev_pricem
           |      ,tt.variance_pricem
           |      ,tt.first_max_pricem
           |      ,tt.first_min_pricem
           |      ,tt.second_max_pricem
           |      ,tt.second_min_pricem
           |      ,(tt.first_max_pricem - tt.second_max_pricem) as first_diff_max_pricem
           |      ,(tt.second_min_pricem - tt.first_min_pricem) as first_diff_min_pricem
           |      ,tt.third_max_pricem
           |      ,tt.third_min_pricem
           |      ,(tt.second_max_pricem -tt.third_max_pricem) as second_diff_max_pricem
           |      ,(tt.third_min_pricem -tt.second_min_pricem) as second_diff_min_pricem
           |      ,tt.data_cnt
           | from(
           |select cid
           |      ,subcid
           |      ,stdname
           |      ,features
           |      ,unit
           |      ,max(pricem) as max_pricem
           |      ,min(pricem) as min_pricem
           |      ,mean(pricem) as mean_pricem
           |      ,stddev(pricem) as stddev_pricem
           |      ,variance(pricem) as variance_pricem
           |      ,(mean(pricem)+1.5*stddev(pricem)) as first_max_pricem
           |      ,(mean(pricem)-1.5*stddev(pricem)) as first_min_pricem
           |      ,(mean(pricem)+1.28*stddev(pricem)) as second_max_pricem
           |      ,(mean(pricem)-1.28*stddev(pricem)) as second_min_pricem
           |      ,(mean(pricem)+1.04*stddev(pricem)) as third_max_pricem
           |      ,(mean(pricem)-1.04*stddev(pricem)) as third_min_pricem
           |      ,count(1) as data_cnt
           | from pdw_db.p_tfacmaterialbase
           |where cid in ('17','18','19','23','28','55','32')
           |  and isdeleted ='0'
           |  and isaudit = '1'
           |  and sourceType !='S'
           |group by cid
           |         ,subcid
           |         ,stdname
           |         ,features
           |         ,unit) tt
         """.stripMargin)
      logger.warn("exec_sql"+exec_sql)
      spark.stop()
  }
}
