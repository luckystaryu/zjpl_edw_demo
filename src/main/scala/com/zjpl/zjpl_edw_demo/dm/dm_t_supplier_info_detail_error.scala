package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.spark_udf.customUDF
import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_t_supplier_info_detail_error {
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
    spark.udf.register("isChineseCharacter",customUDF.isChineseCharacter(_:String))
    sql(
      s"""
         | create table if not exists dm_db.dm_t_supplier_info_detail_error
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
    sql("alter table dm_db.dm_t_supplier_info_detail_error drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table dm_db.dm_t_supplier_info_detail_error drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into dm_db.dm_t_supplier_info_detail_error partition (etl_dt='$yest_dt')
         |select supplier_id
         |,supplier_num
         |,credit_cd
         |,supplier_name
         |,supplier_nick_name
         |,url
         |,logo
         |,legal_representor
         |,regist_capital
         |,currency_cd
         |,supplier_status_cd
         |,regist_date
         |,taxpayer_regist_num
         |,regist_num
         |,org_cd
         |,supplier_type_cd
         |,province_cd
         |,city_cd
         |,industry_name
         |,regist_org_name
         |,term_start_date
         |,term_end_date
         |,supplier_addr
         |,business_scope
         |,is_general_taxpayer
         |,business_license
         |,create_time
         |,creator_name
         |,update_time
         |,updator_name
         |  from pdw_db.p_t_supplier_info
         | where etl_dt='$yest_dt'
         |  and (coalesce(legal_representor,'') =''
         |      or coalesce(city_cd,'') = ''
         |      or coalesce(supplier_status_cd,'')=''
         |      or (isChineseCharacter(city_cd)='1'
         |       or coalesce(city_cd,'')=''))
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
