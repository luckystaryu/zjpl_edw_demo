package com.zjpl.zjpl_edw_demo.sparkstreaming

import scala.collection.mutable.ListBuffer

object JDBCWrapperTest {
  def main(args: Array[String]): Unit = {
    val jdbcWrapper01=JDBCWrapper.getInstance()
    val paramsList = ListBuffer[ParamsList]()
    val paramListTmp = new ParamsList
    paramListTmp.p_num="01"
    paramListTmp.ch_mor_rate=1
    paramListTmp.mor_rate=1
    paramListTmp.risk_rank="02"
    paramListTmp.params_Type = "real_risk"
    paramsList +=paramListTmp
    val insertNum = jdbcWrapper01.doBatch("INSERT INTO real_risk(p_num,ch_mor_rate,mor_rate,risk_rank) VALUE(?,?,?,?)",paramsList)
  }
}
