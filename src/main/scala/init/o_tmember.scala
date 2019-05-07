package init

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tmember {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
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
    val o_tmember_sql =
      s"""
         |(SELECT ID
         |,REPLACE(REPLACE(MemberID,'\n',''),'\r','') AS MemberID
         |,REPLACE(REPLACE(WebArea,'\n',''),'\r','') AS WebArea
         |,REPLACE(REPLACE(PWD,'\n',''),'\r','') AS PWD
         |,REPLACE(REPLACE(TrueName,'\n',''),'\r','') AS TrueName
         |,REPLACE(REPLACE(Sex,'\n',''),'\r','') AS Sex
         |,Degree
         |,REPLACE(REPLACE(PostCode,'\n',''),'\r','') AS PostCode
         |,REPLACE(REPLACE(Addr,'\n',''),'\r','') AS Addr
         |,REPLACE(REPLACE(Email,'\n',''),'\r','') AS Email
         |,REPLACE(REPLACE(QQ,'\n',''),'\r','') AS QQ
         |,REPLACE(REPLACE(MSN,'\n',''),'\r','') AS MSN
         |,REPLACE(REPLACE(Phone,'\n',''),'\r','') AS Phone
         |,REPLACE(REPLACE(Fax,'\n',''),'\r','') AS Fax
         |,REPLACE(REPLACE(Mobile,'\n',''),'\r','') AS Mobile
         |,LastTime
         |,LoginNum
         |,REPLACE(REPLACE(IPAddr,'\n',''),'\r','') AS IPAddr
         |,ValidDate
         |,REPLACE(REPLACE(isVoid,'\n',''),'\r','') AS isVoid
         |,isLock
         |,CreateOn
         |,UpdateOn
         |,REPLACE(REPLACE(UpdateBy,'\n',''),'\r','') AS UpdateBy
         |,REPLACE(REPLACE(Memo,'\n',''),'\r','') AS Memo
         |,REPLACE(REPLACE(CorpName,'\n',''),'\r','') AS CorpName
         |,REPLACE(REPLACE(FID,'\n',''),'\r','') AS FID
         |,regType
         |,REPLACE(REPLACE(MemberType,'\n',''),'\r','') AS MemberType
         |,REPLACE(REPLACE(WebSite,'\n',''),'\r','') AS WebSite
         |,REPLACE(REPLACE(Department,'\n',''),'\r','') AS Department
         |,REPLACE(REPLACE(logo,'\n',''),'\r','') AS logo
         |,searchTimes
         |,REPLACE(REPLACE(EID,'\n',''),'\r','') AS EID
         |,isAdmin
         |,LastDegree
         |,EpLock
         |,REPLACE(REPLACE(RegProvince,'\n',''),'\r','') AS RegProvince
         |,REPLACE(REPLACE(webProvince,'\n',''),'\r','') AS webProvince
         |,REPLACE(REPLACE(WebCounty,'\n',''),'\r','')  AS WebCounty
         |,REPLACE(REPLACE(AccessSite,'\n',''),'\r','') AS AccessSite
         |,Score
         |,REPLACE(REPLACE(province,'\n',''),'\r','') AS province
         |,REPLACE(REPLACE(city,'\n',''),'\r','') AS city
         |,askTotal
         |,auditTime
         |,REPLACE(REPLACE(ordersItem,'\n',''),'\r','') AS ordersItem
         |,REPLACE(REPLACE(ordersItemType,'\n',''),'\r','') AS ordersItemType
         |,status
         |,inquiryNum
         |,useScore
         |,signInCount
         |,currScore
         |,exp
         |,REPLACE(REPLACE(goodAt,'\n',''),'\r','') AS goodAt
         |,REPLACE(REPLACE(nickname,'\n',''),'\r','') AS nickname
         |,frozenScore
         |,role
         |,REPLACE(REPLACE(accountBalance,'\n',''),'\r','') AS accountBalance
         |,REPLACE(REPLACE(defaultbank,'\n',''),'\r','') AS defaultbank
         |,REPLACE(REPLACE(invoiceInfo,'\n',''),'\r','') AS invoiceInfo
         |,REPLACE(REPLACE(major,'\n',''),'\r','') AS major
         |,purchaseScore
         |,materialCount
         |,facCount
         |,REPLACE(REPLACE(curMaterialCount,'\n',''),'\r','') AS curMaterialCount
         |,REPLACE(REPLACE(curFacCount,'\n',''),'\r','') AS curFacCount
         |,qkValidDate
         |,lockedTime
         |,wenkuSigninCount
         |,empStatus
         |,REPLACE(REPLACE(introduction,'\n',''),'\r','') AS introduction
         |,REPLACE(REPLACE(resume,'\n',''),'\r','') AS resume
         |,phoneVerification
         |,matDownloadCount
         |,residuematDownloadCount
         |,authorizationStatus
         |,capacityOfCount
         |,cloudInterestedBuyers
         |,applyForOn
         |,enterpriseCreationOn
         |,REPLACE(REPLACE(position,'\n',''),'\r','') AS position
         |,REPLACE(REPLACE(qqOpenId,'\n',''),'\r','') AS qqOpenId
         |,REPLACE(REPLACE(accessToken,'\n',''),'\r','') AS accessToken
         |,regchannel
         |,qqstatus
         |,authorizationVIP
         |,askCount
         |,residueAskCount
         |,totalAskCount
         |,govExcelCount
         |,REPLACE(REPLACE(supplierID ,'\n',''),'\r','') AS supplierID
         |,assignAskCount
         |,assignGovExcelCount
         |,REPLACE(REPLACE(companyType,'\n',''),'\r','') AS companyType
         |,REPLACE(REPLACE(weixinUnionid,'\n',''),'\r','') AS weixinUnionid
         |,REPLACE(REPLACE(weixinStatus,'\n',''),'\r','') AS weixinStatus
         |,REPLACE(REPLACE(isLayer,'\n',''),'\r','') AS isLayer
         |,REPLACE(REPLACE(masterMemberId,'\n',''),'\r','') AS masterMemberId
         |,age
         |,education
         |,REPLACE(REPLACE(jobTitle,'\n',''),'\r','') AS jobTitle
         |,REPLACE(REPLACE(attentionContent,'\n',''),'\r','') AS attentionContent
         | ,REPLACE(REPLACE(regSource,'\n',''),'\r','') AS regSource
         | from costexpressdb.tMember
         | ) as o_tmember
       """.stripMargin
    logger.warn("o_tmember_sql"+o_tmember_sql)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password =edw_config.properties.getProperty("mysql_zjt.password")
    val mysteel_dataDF = spark.read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",o_tmember_sql)
      .option("quoteAll",true)
      .load()
    mysteel_dataDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("ods_db.o_tmember")
    spark.stop()
  }
}

