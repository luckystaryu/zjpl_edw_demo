package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_supplier_contactor_info {
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
         | create table if not exists pdw_db.p_t_supplier_contactor_info
         |( id                 int
         | ,supplier_num       string     comment '供应商编号'
         | ,contactor_name     string     comment '联系人姓名'
         | ,sex_cd             string     comment '性别代码'
         | ,duty               string     comment '职务'
         | ,contact_type_cd    string     comment '联系方式类型代码'
         | ,contact_type       string     comment '联系方式'
         | ,create_time        timestamp  comment '创建时间'
         | ,creator_name       string     comment '创建人'
         | ,update_time        timestamp  comment '修改时间'
         | ,updator_name       string     comment '修改人'
         |)partitioned by(etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_supplier_contactor_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table pdw_db.p_t_supplier_contactor_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into pdw_db.p_t_supplier_contactor_info partition (etl_dt='$yest_dt')
         |select tt.supplier_id
         |      ,tt.supplier_num
         |      ,tt.contactor_name
         |      ,tt.sex_cd
         |      ,tt.duty
         |      ,split(tt1.contact_type,':')[0] as contact_type_cd
         |      ,split(tt1.contact_type,':')[1] as contact_type
         |      ,tt.create_time
         |      ,tt.creator_name
         |      ,tt.update_time
         |      ,tt.updator_name
         | from (
         |select t1.id     as supplier_id
         |      ,t1.Eid    as supplier_num
         |      ,t1.CONTACT as contactor_name
         |      ,t1.SEX     as sex_cd
         |      ,null       as duty
         |      ,concat_ws(',',case when coalesce(MOBILE,'')!=''
         |                          then concat('01',':',MOBILE)
         |                          else null
         |                      end
         |                    ,case when coalesce(phone,'')!=''
         |                          then concat('02',':',phone)
         |                          else null
         |                      end
         |                    ,case when coalesce(mail,'')!=''
         |                          then concat('06',':',mail)
         |                          else null
         |                      end
         |                    ,case when coalesce(qq,'')!=''
         |                          then concat('05',':',qq)
         |                          else null
         |                      end) as contact_type
         |      ,cast(t1.CreateOn  as timestamp)  as create_time
         |      ,t1.CreateBy                      as creator_name
         |      ,cast(t1.UpdateOn  as timestamp)  as update_time
         |      ,t1.UpdateBy                      as updator_name
         |  from ods_db.o_tepshop_h t1
         | where to_date(t1.start_dt) <= to_date('$yest_dt')
         |   and to_date(t1.end_dt) > to_date('$yest_dt')
         |   and t1.isDeleted !=1
         |   and t1.IsAudit =1
         |   and coalesce(t1.CONTACT,'')!='')tt
         |   LATERAL VIEW explode(split(contact_type,',')) tt1 as contact_type
         |  WHERE regexp_replace(coalesce(split(tt1.contact_type,':')[1],''),' ','')!=''
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
