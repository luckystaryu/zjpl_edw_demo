package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_material_price_info_anhui_zjt {
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
         | create table if not exists pdw_db.p_t_material_price_info_zjt
         |(  Id               int            comment  '自增长主键'
         |,cid             string          comment '分类'
         |,subcid          string          comment '二级分类'
         |,nid             string          comment ''
         |,Addr            string          comment '地址'
         |,Code            string          comment ''
         |,Code03          string          comment ''
         |,Cname           string          comment ''
         |,Name            string          comment '材料名称'
         |,stdName         string          comment '标准名称'
         |,fenci           string          comment '分词匹配项'
         |,Spec            string          comment '规格型号'
         |,Unit            string          comment '单位'
         |,PriceM          decimal         comment '面价'
         |,FID             string          comment '供应商id'
         |,FName           string          comment '供应商名称'
         |,Brand           string          comment '品牌'
         |,IssueDate       timestamp       comment ''
         |,CreateON        timestamp       comment '创建时间'
         |,CreateBy        string          comment '创建人'
         |,UpdateOn        timestamp       comment '更新时间'
         |,UpdateBy        string          comment '更新者'
         |,areacode        string          comment '地区'
         |,Code06          string          comment ''
         |,Code10          string          comment ''
         |,Notes           string          comment '备注'
         |,PriceShow       string          comment '显示的价格'
         |,NameSpec        string          comment '名称规格'
         |,isGov           int             comment '是否信息价'
         |,imageURL        string          comment '价格图片路径'
         |,bianma          string          comment '编码'
         |,features        string          comment '特征项'
         |,keyFeatures     string          comment '关键特征项'
         |,f01             string          comment '特征1'
         |,f02             string          comment '特征2'
         |,f03             string          comment '特征3'
         |,f04             string          comment '特征4'
         |,f05             string          comment '特征5'
         |,f06             string          comment '特征6'
         |,f07             string          comment '特征7'
         |,f08             string          comment '特征8'
         |,f09             string          comment '特征9'
         |,f10             string          comment '特征10'
         |,f11             string          comment '特征11'
         |,f12             string          comment '特征12'
         |,f13             string          comment '特征13'
         |,f14             string          comment '特征14'
         |,f15             string          comment '特征15'
         |,isHandle        int             comment ''
         |,industry        int             comment '行业'
         |,title                               string           comment '信息价标题'
         |,tax_price                           decimal(14,4)    comment '含税价'
         |,no_tax_price                        decimal(14,4)    comment '除税价'
         |,value_added_tax_rate                decimal(14,4)    comment '增值税率'
         |,business_tax_model_price            decimal(14,4)    comment '营业税模式价格'
         |,value_added_tax_model_price         decimal(14,4)    comment '增值税模式价格'
         |)partitioned by (etl_dt string,province_cd string)
         |stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_material_price_info_zjt drop if exists partition (etl_dt='"+yest_dt02+"',province_cd='anhui')")
    sql("alter table pdw_db.p_t_material_price_info_zjt drop if exists partition (etl_dt='"+yest_dt+"',province_cd='anhui')")
    sql(
      s"""
         |insert into table pdw_db.p_t_material_price_info_zjt partition(etl_dt='$yest_dt',province_cd='anhui')
         |select t1.Id
         |,t1.cid
         |,t1.subcid
         |,t1.nid
         |,t1.Addr
         |,t1.Code
         |,t1.Code03
         |,t1.Cname
         |,t1.Name
         |,t1.stdName
         |,t1.fenci
         |,t1.Spec
         |,t1.Unit
         |,t1.PriceM
         |,t1.FID
         |,t1.FName
         |,t1.Brand
         |,t1.IssueDate
         |,t1.CreateON
         |,t1.CreateBy
         |,t1.UpdateOn
         |,t1.UpdateBy
         |,t1.areacode
         |,t1.Code06
         |,t1.Code10
         |,t1.Notes
         |,t1.PriceShow
         |,t1.NameSpec
         |,t1.isGov
         |,t1.imageURL
         |,t1.bianma
         |,t1.features
         |,t1.keyFeatures
         |,t1.f01
         |,t1.f02
         |,t1.f03
         |,t1.f04
         |,t1.f05
         |,t1.f06
         |,t1.f07
         |,t1.f08
         |,t1.f09
         |,t1.f10
         |,t1.f11
         |,t1.f12
         |,t1.f13
         |,t1.f14
         |,t1.f15
         |,t1.isHandle
         |,t1.industry
         |,t2.title
         |,t2.tax_price
         |,t2.no_tax_price
         |,t2.value_added_tax_rate
         |,t2.business_tax_model_price
         |,t2.value_added_tax_model_price
         | from ods_db.o_tgovmaterialbase_anhui_h t1
         | inner join ods_db.o_tGovExtends_anhui_h t2
         |  on t1.id= t2.pid
         | where to_date(t1.etl_dt) =to_date('$yest_dt')
       """.stripMargin)
    spark.close()
    spark.stop()
  }
}
