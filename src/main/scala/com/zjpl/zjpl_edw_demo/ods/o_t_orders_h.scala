package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_t_orders_h {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    if (args.length !=0)
    {
      yest_dt = args(0)
    }else
    {
      val date:Date = new Date()
      val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
    }
    import spark.sql
    sql(
      s"""
         |create table if not exists  ods_db.o_t_orders_h
         |( order_id         string      COMMENT '订单Id，主键',
         |  member_id        string      COMMENT '用户Id，订单购买者，关联tMember.MemberID',
         |  master_member_id string      COMMENT '用户主账号Id，关联tMember.MemberID（冗余）',
         |  order_status     tinyint     COMMENT '订单状态。-1:已关闭、0:已下单、1:已完成',
         |  pay_status       tinyint     COMMENT '支付状态。0未付款、1:已付款',
         |  pay_amount       bigint      COMMENT '订单支付金额，以‘分’为单位，订单支付金额 =  (订单金额 + 快递费) - 折扣额',
         |  order_amount     int         COMMENT '订单金额，以‘分’为单位',
         |  express_fee      int         COMMENT '快递费，以‘分’为单位',
         |  discount         int         COMMENT '折扣额，以‘分’为单位',
         |  order_source     tinyint     COMMENT '订单来源。1:PC端、2:移动端、3:线下工单',
         |  pay_way          tinyint     COMMENT '支付方式（冗余）。0:T币、1:支付宝、2:维信扫码支付、3:公众号支付、4:无/站位、5:积分兑换、6:银行卡',
         |  create_on        timestamp   COMMENT '创建时间',
         |  pay_time         timestamp   COMMENT '支付时间',
         |  deliver_time     timestamp   COMMENT '发货时间',
         |  deliver_status   tinyint     COMMENT '发货状态。0:未发货、1:已发货',
         |  recipients       string      COMMENT '收件人',
         |  contact_number   string      COMMENT '联系电话',
         |  address          string      COMMENT '详细地址',
         |  remark           string      COMMENT '备注',
         |  express_company  string      COMMENT '快递公司',
         |  express_number   string      COMMENT '快递单号',
         |  invoice_status   tinyint     COMMENT '发票状态。-1:无需发票、0:待开发票、1:已处理',
         |  invoice_record_id int        COMMENT '发票记录Id，关联t_invoice_record.id',
         |  corp_name        string      COMMENT '公司名称',
         |  true_name        string      COMMENT '联系人',
         |  phone            string      COMMENT '联系电话',
         |  sales_man        string      COMMENT '销售员',
         |  deletes          int         COMMENT '0:未删除,1已删除',
         |  pay_remind       tinyint     COMMENT '支付提醒。0:未提醒、1:已提醒。注：当下单并未支付时就会提醒用户支付',
         |  post_code        string      COMMENT '邮政编码',
         |  pay_limit_time   timestamp   COMMENT '支付时间期限。超过该时间后就不能支付了。当该字段为空时表示默认7天',
         |  audit_person     string      COMMENT '审核人。工单最后审核人',
         |  return_tb        int         COMMENT 'T币返现额，以‘分’为单位'
         |)stored as parquet
       """.stripMargin)
    sql("alter table ods_db.o_t_orders_h drop if exists partition (etl_dt='" + yest_dt + "')")
    sql(
      s"""
         |insert overwrite table ods_db.o_t_orders_h (etl_dt='$yest_dt')
         |select order_id
         |,member_id
         |,master_member_id
         |,order_status
         |,pay_status
         |,pay_amount
         |,order_amount
         |,express_fee
         |,discount
         |,order_source
         |,pay_way
         |,create_on
         |,pay_time
         |,deliver_time
         |,deliver_status
         |,recipients
         |,contact_number
         |,address
         |,remark
         |,express_company
         |,express_number
         |,invoice_status
         |,invoice_record_id
         |,corp_name
         |,true_name
         |,phone
         |,sales_man
         |,deletes
         |,pay_remind
         |,post_code
         |,pay_limit_time
         |,audit_person
         |,return_tb
         |from ods_db.o_t_orders
       """.stripMargin)
    spark.stop()
  }
}
