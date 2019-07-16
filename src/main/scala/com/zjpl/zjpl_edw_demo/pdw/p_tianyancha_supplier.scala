package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object p_tianyancha_supplier {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
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
         |create table if not exists pdw_db.p_tianyancha_supplier
         |(search_name              string    comment '源公司名'
         |,company_name             string    comment '搜索结果公司名'
         |,legal_person             string    comment '企业法人'
         |,registered_date          string    comment '注册时间'
         |,registered_capital       decimal(18,2)    comment '注册资本'
         |,currency_cn_name         string    comment '币种代码'
         |,enterprise_type          string    comment '所属行业'
         |,company_type             string    comment '公司类型'
         |,registration_number      string    comment '公司注册号'
         |,organization_code        string    comment '组织结构代码'
         |,credit_code              string    comment '统一信用代码'
         |,tax_reg_num              string    comment '纳税人识别号'
         |,term                     string    comment '经营期限'
         |,establishment_date       string    comment '核准日期'
         |,registration_authority   string    comment '登记机关'
         |,address                  string    comment '注册地址'
         |,status                   string    comment '企业状态'
         |,phone                    string    comment '联系方式'
         |,website                  string    comment '网站地址'
         |,management               string    comment '经营范围'
         |) partitioned by (etl_dt string)
         |stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_tianyancha_supplier drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table pdw_db.p_tianyancha_supplier drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into  table pdw_db.p_tianyancha_supplier partition (etl_dt='$yest_dt')
         |select t1.search_name
         |,t1.company_name
         |,t1.legal_person
         |,t1.registered_date
         |,cast(substr(t1.registered_capital,1,instr(t1.registered_capital,'万')-1) as decimal(18,2)) as registered_capital
         |,case when substr(t1.registered_capital,instr(t1.registered_capital,'万')+1) in ('元人民币','元')
         |      then '人民币'
         |      else substr(t1.registered_capital,instr(t1.registered_capital,'万')+1)
         |  end as currency_cn_name
         |,t1.enterprise_type
         |,t1.company_type
         |,t1.registration_number
         |,t1.organization_code
         |,t1.credit_code
         |,t1.tax_reg_num
         |,t1.term
         |,t1.establishment_date
         |,t1.registration_authority
         |,t1.address
         |,coalesce(t2.supplier_status_cd,case when t1.status='开业'
         |                                     then '01'
         |                                     when t1.status='核准设立'
         |                                     then '02'
         |                                     when t1.status='撤销'
         |                                     then '03'
         |                                     when t1.status='废止'
         |                                     then '06'
         |                                     when t1.status in ('已告解散','解散')
         |                                     then '08'
         |                                     else t1.status
         |                                end) as supplier_status_cd
         |,t1.phone
         |,t1.website
         |,t1.management
         | from ods_db.o_tianyancha_supplier_h t1
         | left join ods_db.o_t_supplier_status_cd_h t2
         |    on t1.status = t2.supplier_status_name
         |   and to_date(t2.start_dt) <=to_date('$yest_dt')
         |   and to_date(t2.end_dt) > to_date('$yest_dt')
         |where to_date(t1.start_dt) <=to_date('$yest_dt')
         |   and to_date(t1.end_dt) > to_date('$yest_dt')
       """.stripMargin)
    spark.stop()
  }
}
