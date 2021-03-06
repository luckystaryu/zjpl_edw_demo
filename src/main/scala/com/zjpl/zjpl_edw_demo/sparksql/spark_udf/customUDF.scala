package com.zjpl.zjpl_edw_demo.sparksql.spark_udf

import java.util.regex.{Matcher, Pattern}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.Chr

object customUDF {
  def cityNameUDF(city:String): String ={
    var city_new:String= ""
    if(StringUtils.isNotBlank(city)&&city.endsWith("市市")){
      city_new = city.substring(0,city.length-1)
    }else if(StringUtils.isNotBlank(city)&&city.contains("省")){
      city_new=city.substring(city.indexOf("省")+1)
    }else{
      city_new=city.concat("市")
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
  def convertProvinceUDF(province:String): String ={
    var province_new =""
    if(province.equals("ah")){
      province_new="安徽"
    }else if(province.equals("bj")){
      province_new="北京"
    }else if(province.equals("fj")){
      province_new="福建"
    }else if(province.equals("gs")){
      province_new="甘肃"
    }else if(province.equals("gd")){
      province_new="广东"
    }else if(province.equals("gx")){
      province_new="广西"
    }else if(province.equals("gz")){
      province_new="贵州"
    }else if(province.equals("hainan")){
      province_new="海南"
    }else if(province.equals("hebei")){
      province_new="河北"
    }else if(province.equals("henan")){
      province_new="河南"
    }else if(province.equals("hlj")){
      province_new="黑龙江"
    }else if(province.equals("hubei")){
      province_new="湖北"
    }else if(province.equals("hunan")){
      province_new="湖南"
    }else if(province.equals("jl")){
      province_new="吉林"
    }else if(province.equals("js")){
      province_new="江苏"
    }else if(province.equals("jx")){
      province_new="江西"
    }else if(province.equals("ln")){
      province_new="辽宁"
    }else if(province.equals("nmg")){
      province_new="内蒙古"
    }else if(province.equals("nx")){
      province_new="宁夏"
    }else if(province.equals("qh")){
      province_new="青海"
    }else if(province.equals("sd")){
      province_new="山东"
    }else if(province.equals("shanxi")){
      province_new="山西"
    }else if(province.equals("sx")){
      province_new="陕西"
    }else if(province.equals("sh")){
      province_new="上海"
    }else if(province.equals("sc")){
      province_new="四川"
    }else if(province.equals("tj")){
      province_new="天津"
    }else if(province.equals("xz")){
      province_new="西藏"
    }else if(province.equals("xj")){
      province_new="新疆"
    }else if(province.equals("yn")){
      province_new="云南"
    }else if(province.equals("zj")){
      province_new="浙江"
    }else if(province.equals("cq")){
      province_new="重庆"
    }
    province_new
  }

  /**
    * 判断字符串中是否包含中文
    * @param inputStr
    * @return
    */
  def isChineseCharacter(inputStr:String): String ={
    val pattern:Pattern=Pattern.compile("[\\u4e00-\\u9fa5]")
    var result:String="0"
    if(StringUtils.isNotBlank(inputStr)){
      val m:Matcher=pattern.matcher(inputStr)
      if(m.find()){
        result="1"
      }else{
        result="0"
      }
    }
    result
  }

  /**
    * 判断字符串中是否是纯数字
    * @param inputStr
    * @return
    */
  def isInteger(inputStr:String):Int = {
    var result:Int=0
    if(StringUtils.isNotBlank(inputStr)){
      val parttern:Pattern=Pattern.compile("^[-\\+]?[\\d]*$")
      val m:Matcher=parttern.matcher(inputStr)
      if(m.matches()){
        result=1
      }else{
        result=0
      }
    }
    result
  }

  /***
    * 判断字符串是否是邮箱
    * @param inputStr
    * @return
    */
  def isEmail(inputStr:String):Int ={
    var result:Int=0
    if(StringUtils.isNotBlank(inputStr)){
      val pattern:Pattern=Pattern.compile("^[a-zA-Z0-9_.-]+@[a-zA-Z0-9-]+(\\.[a-zA-Z0-9-]+)*\\.[a-zA-Z0-9]{2,6}$")
      val m:Matcher=pattern.matcher(inputStr)
      if(m.matches()){
        result=1
      }else{
        result=0
      }
    }
    result
  }
  def defineReplace(inputStr:String):String= {
    var resultStr: String = ""
    if (StringUtils.isNotBlank(inputStr)) {
      resultStr = inputStr.trim.replaceAll("\\n|\\t|\\r|\\r\\n|\\n\\r|-","").replace(" ","")
    }
    resultStr
  }
}
