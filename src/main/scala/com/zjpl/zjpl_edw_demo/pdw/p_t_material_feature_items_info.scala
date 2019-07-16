package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_material_feature_items_info {
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
         | create table if not exists pdw_db.p_t_material_feature_items_info
         |(id	int	                        comment'序号'
         |,material_num       	string    comment'材料二级编码'
         |,material_feature_id	string    comment'材料特征序号'
         |,material_feature_name	string  comment'材料特征名称'
         |,is_key_feature	string          comment'是否关键特征'
         |,create_time	    timestamp     comment'创建时间'
         |,creator_name	  string          comment'创建人'
         |,update_time	    timestamp     comment'修改时间'
         |,updator_name	  string          comment'修改人'
         |)stored as parquet
       """.stripMargin)
    val p_t_material_feature_items_infoDF=sql(
      s"""
         |select std_code
         |      ,attribute_code
         |      ,feature
         |      ,keyfeature
         |      ,current_timestamp as create_time
         |      ,'sys'   as creator_name
         |      ,current_timestamp as update_time
         |      ,'sys' as updator_name
         | from ods_db.o_T_FEATURE t1
       """.stripMargin)
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number
    val w =Window.orderBy("std_code","attribute_code")
    val p_t_material_feature_items_infoDF01=p_t_material_feature_items_infoDF.withColumn("id",row_number()over(w))
    p_t_material_feature_items_infoDF01
        .select("id","std_code","feature","keyfeature","create_time","creator_name","update_time","updator_name")
      .write.mode(SaveMode.Overwrite).saveAsTable("pdw_db.p_t_material_feature_items_info")
    spark.stop()
    spark.close()
  }
}
