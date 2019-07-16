package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tianyancha_supplier_h {
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
         |create table if not exists ods_db.o_tianyancha_supplier_h
         |(start_dt                 string comment '开始时间'
         |,end_dt                   string comment '结束时间'
         |,search_name              string    comment '源公司名'
         |,company_name             string    comment '搜索结果公司名'
         |,legal_person             string    comment '企业法人'
         |,registered_date          string    comment '注册时间'
         |,registered_capital       string    comment '注册资本'
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
         |) stored as parquet
       """.stripMargin)
    val o_tianyancha_supplier_h_tmp=sql (
      s"""
         |select t1.start_dt
         |,t1.end_dt
         |,t1.search_name
         |,t1.company_name
         |,t1.legal_person
         |,t1.registered_date
         |,t1.registered_capital
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
         |,t1.status
         |,t1.phone
         |,t1.website
         |,t1.management
         | from ods_db.o_tianyancha_supplier_h t1
         | where to_date(t1.start_dt) <= to_date('$yest_dt')
       """.stripMargin)
    sql("drop table if exists ods_db.o_tianyancha_supplier_h_tmp purge")
    o_tianyancha_supplier_h_tmp.write.mode(SaveMode.Overwrite).saveAsTable("ods_db.o_tianyancha_supplier_h_tmp")

    /**
      * 1.将变动的数据全部只为失效
      */
    val o_tianyancha_supplier_h_tmp01=sql(
      s"""
         |insert overwrite table ods_db.o_tianyancha_supplier_h
         |select
         | t1.start_dt
         |,to_date('$yest_dt') as end_dt
         |,t1.search_name
         |,t1.company_name
         |,t1.legal_person
         |,t1.registered_date
         |,t1.registered_capital
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
         |,t1.status
         |,t1.phone
         |,t1.website
         |,t1.management
         |  from ods_db.o_tianyancha_supplier_h_tmp t1
         |  left semi join ods_db.o_tianyancha_supplier t2
         |    on t1.search_name = t2.search_name
       """.stripMargin
    )
    val o_tianyancha_supplier_h_tmp02=sql(
      s"""
         |insert into table ods_db.o_tianyancha_supplier_h
         |select
         | '$yest_dt' as start_dt
         |,'2999-12-31' as end_dt
         |,t1.search_name
         |,t1.company_name
         |,t1.legal_person
         |,t1.registered_date
         |,t1.registered_capital
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
         |,t1.status
         |,t1.phone
         |,t1.website
         |,t1.management
         |from ( select id
         |,search_name
         |,company_name
         |,legal_person
         |,registered_date
         |,registered_capital
         |,enterprise_type
         |,company_type
         |,registration_number
         |,organization_code
         |,credit_code
         |,tax_reg_num
         |,term
         |,establishment_date
         |,registration_authority
         |,address
         |,status
         |,phone
         |,website
         |,management
         |,date
         |,row_number()over(partition by search_name order by id desc) as rn
         | from  ods_db.o_tianyancha_supplier) t1
         | where rn =1
       """.stripMargin
    )
    val o_tianyancha_supplier_h_tmp03 = sql(
      s"""
         |insert into table ods_db.o_tianyancha_supplier_h
         |select
         | t1.start_dt
         |,t1.end_dt
         |,t1.search_name
         |,t1.company_name
         |,t1.legal_person
         |,t1.registered_date
         |,t1.registered_capital
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
         |,t1.status
         |,t1.phone
         |,t1.website
         |,t1.management
         |  from ods_db.o_tianyancha_supplier_h_tmp t1
         |  left join ods_db.o_tianyancha_supplier t2
         |    on t1.search_name = t2.search_name
         | where t2.search_name is null
       """.stripMargin
    )
    sql("drop table if exists ods_db.o_tianyancha_supplier_h_tmp purge")
    spark.stop()
  }
}
