package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tfacmatbrand_h {
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
         |create table if not exists ods_db.o_tfacmatbrand_h(
         |start_dt    string comment '开始时间'
         |,end_dt      string comment '结束时间'
         |,id              int        comment '序号'
         |,name            string     comment '品牌名称'
         |,grade           string     comment '等级'
         |,frontlineAreas  string     comment ''
         |,imgURL          string     comment '图片'
         |,isDeleted       string     comment '是否删除 0:否,1:是'
         |,matCount        int        comment ''
         |,createOn        timestamp  comment '创建时间'
         |,createBy        string     comment '创建人'
         |,updateOn        timestamp  comment '更新时间'
         |,updateBy        string     comment '更新人'
         |)stored as parquet
       """.stripMargin)

    /**
      * 1.将历史数据保留到临时表中
      */
    val o_tfacmatbrand_h_tmp=sql(
      s"""
         | select t1.start_dt
         |,t1.end_dt
         |,t1.id
         |,t1.name
         |,t1.grade
         |,t1.frontlineAreas
         |,t1.imgURL
         |,t1.isDeleted
         |,t1.matCount
         |,t1.createOn
         |,t1.createBy
         |,t1.updateOn
         |,t1.updateBy
         |  from ods_db.o_tfacmatbrand_h t1
         | where t1.start_dt <'$yest_dt'
       """.stripMargin)
    sql("drop table if exists ods_db.o_tfacmatbrand_h_tmp purge")
    o_tfacmatbrand_h_tmp.write.mode(SaveMode.Overwrite).saveAsTable("ods_db.o_tfacmatbrand_h_tmp")
    /**
      * 2.将历史变动的数据全部置为失效
      */
    val o_tfacmatbrand_h_tmp01=sql(
      s"""
         |insert overwrite table ods_db.o_tfacmatbrand_h
         |select t1.start_dt
         |,'$yest_dt' as end_dt
         |,t1.id
         |,t1.name
         |,t1.grade
         |,t1.frontlineAreas
         |,t1.imgURL
         |,t1.isDeleted
         |,t1.matCount
         |,t1.createOn
         |,t1.createBy
         |,t1.updateOn
         |,t1.updateBy
         |  from ods_db.o_tfacmatbrand_h_tmp t1
         |  left semi join ods_db.o_tfacmatbrand t2
         |   on t1.id = t2.id
       """.stripMargin)
    /**
      * 2.将变动(存量DDL和新增数据）的数据写入临时表中
      */
    val o_t_supplier_status_cd_h_tmp02=sql(
      s"""
         |insert into table ods_db.o_tfacmatbrand_h
         |select '$yest_dt' as start_dt
         |,'2999-12-31' as end_dt
         |,t1.id
         |,t1.name
         |,t1.grade
         |,t1.frontlineAreas
         |,t1.imgURL
         |,t1.isDeleted
         |,t1.matCount
         |,t1.createOn
         |,t1.createBy
         |,t1.updateOn
         |,t1.updateBy
         | from ods_db.o_tfacmatbrand t1
       """.stripMargin)

    /**
      * 3.将存量数据写入目标表
      */
    val o_t_supplier_status_cd_h_tmp03=sql(
      s"""
         |insert into table ods_db.o_tfacmatbrand_h
         |select t1.start_dt
         |,t1.end_dt
         |,t1.id
         |,t1.name
         |,t1.grade
         |,t1.frontlineAreas
         |,t1.imgURL
         |,t1.isDeleted
         |,t1.matCount
         |,t1.createOn
         |,t1.createBy
         |,t1.updateOn
         |,t1.updateBy
         | from ods_db.o_tfacmatbrand_h_tmp t1
         | left join  ods_db.o_tfacmatbrand  t2
         |  on t1.id= t2.id
         | where t2.id is null
       """.stripMargin
    )
    sql("drop table if exists ods_db.o_tfacmatbrand_h_tmp purge")
    spark.stop()
    spark.close()
  }
}
