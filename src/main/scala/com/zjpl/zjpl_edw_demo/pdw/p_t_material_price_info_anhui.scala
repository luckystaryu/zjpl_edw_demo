package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory


object p_t_material_price_info_anhui {
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
         | create table if not exists pdw_db.p_t_material_price_info
         |(id                    int	            comment '材料价格序号'
         |,material_num          string        comment '材料编码'
         |,brand_num             string     comment '品牌编号'
         |,unit                  string      comment '单位'
         |,is_taxes              string         comment '是否包含税费'
         |,is_transport_fee      string         comment '是否包含运输费'
         |,is_manage_fee         string         comment '是否包含管理费'
         |,is_loss_fee           string         comment '是否包含损耗费'
         |,bare_unit_price       decimal(18,4)      comment '裸单价'
         |,unit_price            decimal(18,4)      comment '单价'
         |,currency_cd           string         comment '货币代码'
         |,price_indices_cd      string         comment '价格指数代码'
         |,publisher_num         string         comment '发布方编号'
         |,issue_date            string         comment '发布日期'
         |,valide_date           string        comment '有效日期'
         |,area_name               string        comment '发布行政区划名称'
         |,create_time           timestamp     comment '创建时间'
         |,creator_name          string     comment '创建人'
         |,update_time           timestamp    comment '修改时间'
         |,updator_name          string     comment '修改人'
         |)partitioned by (etl_dt string,province_cd string)
         |stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_material_price_info drop if exists partition (etl_dt='"+yest_dt02+"',province_cd='34')")
    sql("alter table pdw_db.p_t_material_price_info drop if exists partition (etl_dt='"+yest_dt+"',province_cd='34')")
    sql(
      s"""
         |insert into table pdw_db.p_t_material_price_info partition(etl_dt='$yest_dt',province_cd='34')
         |select t1.id
         |     ,t1.Code10
         |     ,t1.Brand
         |     ,t1.Unit
         |     ,'1' as is_taxes
         |     ,'1' as is_transport_fee
         |     ,'1' as is_manage_fee
         |     ,'1' as is_loss_fee
         |     ,t5.no_tax_price  as bare_unit_price
         |     ,t5.tax_price     as unit_price
         |     ,'CNY'            as currency_cd
         |     ,'01'             as price_indices_cd
         |     ,t2.regioncode    as publisher_num
         |     ,t1.issuedate
         |     ,case when dayofmonth(t1.issuedate)=5
         |           then last_day(t1.issuedate)
         |           when dayofmonth(t1.issuedate)=15 and month(issuedate) in ('1','4','7','10')
         |           then last_day(add_months(to_date(t1.issuedate),2))
         |           when dayofmonth(t1.issuedate)=15 and month(issuedate) in ('2','5','8','11')
         |           then last_day(add_months(to_date(t1.issuedate),1))
         |           when dayofmonth(t1.issuedate)=15 and month(issuedate) in ('3','6','9','12')
         |           then last_day(t1.issuedate)
         |           when dayofmonth(t1.issuedate)=25
         |           then concat(year(t1.issuedate),'-12-31')
         |           else null
         |      end as valide_date
         |     ,t1.addr
         |     ,t1.CreateBy
         |     ,t1.CreateON
         |     ,t1.UpdateOn
         |     ,t1.UpdateBy
         | from ods_db.o_tgovmaterialbase_anhui_h t1
         |inner join ods_db.o_tGovExtends_anhui_h t5
         |  on t1.id = t5.id
         | and to_date(t1.etl_dt)=to_date('$yest_dt')
         | left join ods_db.o_tareamanagement_h t2
         |   on t1.areacode = t2.name
         |  and (length(t2.regioncode)=6
         |     or length(t2.regioncode)=2)
         |  and to_date(t2.start_dt) <=to_date('$yest_dt')
         |  and to_date(t2.end_dt) >to_date('$yest_dt')
         | left join ods_db.o_tareamanagement_h t3
         |   on t1.addr = t3.name
         |  and (length(t3.regioncode)=6
         |     or length(t3.regioncode)=2)
         |  and to_date(t3.start_dt) <=to_date('$yest_dt')
         |  and to_date(t3.end_dt) >to_date('$yest_dt')
         |left join  ods_db.o_tfacmatbrand_h t4
         |  on t1.brand = t4.name
         | and to_date(t4.start_dt) <=to_date('$yest_dt')
         | and to_date(t4.end_dt) >to_date('$yest_dt')
         |where to_date(t1.etl_dt)=to_date('$yest_dt')
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
