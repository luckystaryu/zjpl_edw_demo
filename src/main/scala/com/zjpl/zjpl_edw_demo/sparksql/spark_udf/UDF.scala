package com.zjpl.zjpl_edw_demo.sparksql.spark_udf

import java.io.FileInputStream
import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringUtils
import org.apache.poi.ss.usermodel.{Cell, WorkbookFactory}

import scala.collection.mutable.ListBuffer

@deprecated
object UDF {
  def cityNameUDF(city:String): Unit ={
    var city_new = ""
    if(StringUtils.isNotBlank(city)&&city.endsWith("市")){
      city_new = city.substring(0,city.length-1)
    }
    city_new
  }
  def provinceNameUDF(province:String): Unit ={
    var province_new =""
    if(StringUtils.isNotBlank(province)&&province.endsWith("省")){
       province_new =province.substring(0,province.length-1)
    }
    province_new
  }
}
