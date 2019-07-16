package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_supplier_info {
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
         | create table if not exists pdw_db.p_t_supplier_info
         |( supplier_id          int           comment '供应商ID'
         | ,supplier_num         string        comment '供应商编号'
         | ,credit_cd            string        comment '统一社会信用代码'
         | ,supplier_name        string        comment '供应商名称'
         | ,supplier_nick_name   string        comment '供应商简称'
         | ,url                  string        comment '网站地址'
         | ,logo                 string        comment '企业LOGO图片'
         | ,legal_representor    string        comment '法定代表人'
         | ,regist_capital       decimal(18,2) comment '注册资金(万元)'
         | ,currency_cd          string        comment '货币数字代码'
         | ,supplier_status_cd   string        comment '经营状态代码'
         | ,regist_date          string          comment '成立日期'
         | ,taxpayer_regist_num  string        comment '纳税人识别号'
         | ,regist_num           string        comment '注册号'
         | ,org_cd               string        comment '组织机构代码'
         | ,supplier_type_cd     string        comment '企业类型代码'
         | ,province_cd          string        comment '省份代码'
         | ,city_cd              string        comment '城市代码'
         | ,industry_name        string        comment '所属行业'
         | ,regist_org_name      string        comment '登记机关名称'
         | ,term_start_date      string        comment '经营期限起始日期'
         | ,term_end_date        string        comment '经营期限截止日期'
         | ,supplier_addr        string        comment '企业地址'
         | ,business_scope       string        comment '经营范围'
         | ,is_general_taxpayer  string        comment '是否一般纳税人'
         | ,business_license     string        comment '经营执照图片'
         | ,create_time          timestamp     comment '创建时间'
         | ,creator_name         string        comment '创建人'
         | ,update_time          timestamp     comment '修改时间'
         | ,updator_name         string        comment '修改人'
         |)partitioned by(etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_supplier_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table pdw_db.p_t_supplier_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into pdw_db.p_t_supplier_info partition (etl_dt='$yest_dt')
         |select t1.id     as supplier_id
         |      ,t1.Eid    as supplier_num
         |      ,t1.creditCode as credit_cd
         |      ,t1.Name       as supplier_name
         |      ,null          as supplier_nick_name
         |      ,t1.homePage   as url
         |      ,t1.logo       as logo
         |      ,t1.CORPN      as legal_representor
         |      ,coalesce(t1.REGCAPITAL,t4.registered_capital) as regist_capital
         |      ,case when t1.REGCAPITAL is not null or t4.registered_capital is null
         |            then t1.REGCAPITALUNIT
         |            else coalesce(t5.currency_cd,t4.currency_cn_name)
         |         end as currency_cd
         |      ,t4.status          as supplier_status_cd
         |      ,t1.REGTIME         as regist_date
         |      ,t4.tax_reg_num     as taxpayer_regist_num
         |      ,t1.REGNUM          as regist_num
         |      ,t4.organization_code  as org_cd
         |      ,t1.ENTERPRISETYPE  as supplier_type_cd
         |      ,t3.regionCode      as province_cd
         |      ,t2.regionCode      as city_cd
         |      ,t4.enterprise_type as industry_name
         |      ,t1.REGOFFICE       as regist_org_name
         |      ,t1.STARTTIME       as term_start_date
         |      ,t1.ENDTIME         as term_end_date
         |      ,t1.REGADDRESS      as supplier_addr
         |      ,t1.BUSINESSSCOPE   as business_scope
         |      ,null               as is_general_taxpayer
         |      ,t1.BUSINESSLICENSEPIC as business_license
         |      ,cast(t1.CreateOn  as timestamp)  as create_time
         |      ,t1.CreateBy                      as creator_name
         |      ,cast(t1.UpdateOn  as timestamp)  as update_time
         |      ,t1.UpdateBy                      as updator_name
         |  from ods_db.o_tepshop_h t1
         |  left join ods_db.o_tareamanagement_h t2
         |    on t1.city = t2.name
         |   and length(t2.regionCode)=4
         |  left join ods_db.o_tareamanagement_h t3
         |    on t1.province = t3.name
         |   and length(t3.regionCode)=2
         |  left join pdw_db.p_tianyancha_supplier t4
         |    on t1.name = t4.company_name
         |   and t4.search_name= t4.company_name
         |   and to_date(t4.etl_dt) = to_date('$yest_dt')
         |  left join ods_db.o_t_currency_cd_h t5
         |    on t4.currency_cn_name = t5.currency_cn_name
         |   and to_date(t5.start_dt) <= to_date('$yest_dt')
         |   and to_date(t5.end_dt) > to_date('$yest_dt')
         | where to_date(t1.start_dt) <= to_date('$yest_dt')
         |   and to_date(t1.end_dt) > to_date('$yest_dt')
         |   and t1.isDeleted !=1
         |   and t1.IsAudit =1
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
