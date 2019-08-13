package com.zjpl.zjpl_edw_demo.sparksql.sparkToPhoenix



import org.slf4j
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.phoenix.spark._


object a_mp_fac_avg_range {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    val hiveDF = sql("SELECT NEW_MATERIAL_CD,STD_MATERIAL_NAME,PROVINCE_NAME,CITY_NAME,MATERIAL_FACE_PRICE_AVG,ENGIN_PRICE_AVG,RECOMM_PRICE_AVG,MATERIAL_FACE_PRICE_MAX,MATERIAL_FACE_PRICE_MIN,ENGIN_PRICE_MAX,ENGIN_PRICE_MIN,RECOMM_PRICE_MAX,RECOMM_PRICE_MIN,GOV_MATERIAL_FACE_PRICE FROM DM_DB.DM_MP_FAC_AVG_RANGE")
//    hiveDF.write.format("org.apache.phoenix.spark")
//      .mode(SaveMode.Overwrite)
//      .option("table","APP_DB.A_MP_FAC_AVG_RANGE01")
//      .option("zkUrl","172.16.1.62:2181")
//      .save()
    hiveDF.saveToPhoenix(Map("table" -> "APP_DB.A_MP_FAC_AVG_RANGE01", "zkUrl" -> "172.16.1.62:2181"))
    spark.stop()
  }
}
