package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_t_orders_items_h {
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
         |create table if not exists  ods_db.o_t_orders_items_h
         |( id                        int          COMMENT '订单项Id，主键，自增',
         |  order_id                  string       COMMENT '订单Id，关联t_orders.order_id',
         |  package_id                string       COMMENT '套餐id，关联t_package_info.package_id',
         |  package_name              string       COMMENT '套餐名称（冗余）',
         |  price                     int          COMMENT '套餐单价,以‘分’单位',
         |  quantity                  int          COMMENT '套餐购买数量',
         |  region                    string       COMMENT '服务区域。例如：全国、湖南',
         |  marketing_program_id      int          COMMENT '活动方案Id，关联t_marketing_program.id。注：表示用户在下单时享受那个活动优惠',
         |  discount_rate             decimal(3,2) COMMENT '折扣率。以‘折’为单位。冗余t_rel_marketing_program_package.discount_rate。例如：8.5折',
         |  discount_price            int          COMMENT '套餐折后单价，以‘分’单位',
         |  package_member_share_code string       COMMENT '套餐分享码，关联t_package_member_share.share_code',
         |  sso_memo                  string       COMMENT 'SSO人员对订单项修改备忘'
         |)partitioned by (etl_dt string)
         |stored as parquet
       """.stripMargin)
    sql("alter table ods_db.o_t_orders_items_h drop if exists partition (etl_dt ='"+yest_dt+"')")
    sql(
      s"""
         |insert overwrite table ods_db.o_t_orders_items_h partition(etl_dt='$yest_dt')
         |select id
         |,order_id
         |,package_id
         |,package_name
         |,price
         |,quantity
         |,region
         |,marketing_program_id
         |,discount_rate
         |,discount_price
         |,package_member_share_code
         |,sso_memo
         |from ods_db.o_t_orders_items
       """.stripMargin)
    spark.stop()
  }
}
