package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * ./spark2-submit --master local[2] --class com.zjpl.zjpl_edw_demo.sparksql.o_tfacmaterialbase --name o_tfacmaterialbase /data/zjpl/yxq/zjpl_edw_demo-1.0-SNAPSHOT.jar
 */



object o_tepshop {
  def main(args: Array[String]): Unit = {

    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("o_tfacmaterialbase")
      .master("local[*]")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    /**
      * 1.创建hive表
      */
//    sql(s"Create table if not exists odm_db.o_tepshop(" +
//        s" id int comment '自增id'," +
//        s" eid string comment '企业标识'," +
//        s" name string comment '企业名称'," +
//        s" addr string comment '厂商地址'," +
//        s" postcode string comment '邮政地址'," +
//        s" homepage string comment '网站主页'," +
//        s" corpn string comment '企业法人'," +
//        s" erea string comment '法人所在地区'," +
//        s" logo string comment '企业'," +
//        s" longlogo string ," +
//        s" adurl string," +
//        s" culture string comment '企业文化'," +
//        s" bglogo string comment '背景图片'," +
//        s" linkman string comment '其他联系人的相关信息'," +
//        s" mainmaterial string comment '主营材料'," +
//        s" salearea string comment '销售区域'," +
//        s" branchinfo string comment '品牌信息',"+
//        s" provsort string comment '地区排序',"+
//        s" province string comment '省份',"+
//        s" city string comment '城市',"+
//        s" picpath string comment '企业图片地址',"+
//        s" sqspath string comment '授权书图片地址',"+
//        s" brand string ,"+
//        s" discription string comment '描述',"+
//        s" regarea string comment '注册地',"+
//        s" regaddress string comment '注册地址',"+
//        s" regcapital decimal(14,2)  comment '注册资金',"+
//        s" regtime timestamp  comment '注册时间',"+
//        s" regoffice string comment '注册单位',"+
//        s" starttime timestamp  comment '开始时间',"+
//        s" endtime timestamp  comment '结束时间',"+
//        s" regnum string comment '注册号',"+
//        s" businessscope string comment '经营范围',"+
//        s" businesslicensepic string comment '营业执照图片地址',"+
//        s" businessmode tinyint  comment '1生产厂家2经销批发 3商业服务 4招商代理 5其他',"+
//        s" enterprisetype string comment '企业类型',"+
//        s" shoptype string ,"+
//        s" createon timestamp ,"+
//        s" createby string ,"+
//        s" updateon timestamp ,"+
//        s" updateby string ,"+
//        s" islock tinyint ,"+
//        s" isaudit tinyint ,"+
//        s" isdeleted int  comment '删除(1-删除)',"+
//        s" auditor string ,"+
//        s" effectdate timestamp  comment '诚信联盟有效时间',"+
//        s" auditdate timestamp  comment '最新审核时间',"+
//        s" validdate timestamp ,"+
//        s" isvolid tinyint ,"+
//        s" manageid string ,"+
//        s" degree int  ,"+
//        s" cid string ,"+
//        s" subcid string ,"+
//        s" catalogid string ,"+
//        s" grade string ,"+
//        s" hasunauditmat int ,"+
//        s" matcount int ,"+
//        s" contact string ,"+
//        s" sex string ,"+
//        s" department string ,"+
//        s" mobile string ,"+
//        s" phone string ,"+
//        s" isintegrity string ,"+
//        s" creditscore int  comment '诚信积分',"+
//        s" authcontent string comment '认证年限',"+
//        s" recommendcontent string comment '采购商推荐',"+
//        s" putcount int  comment '入库数',"+
//        s" certifnum string comment '认证证书编号',"+
//        s" fax string ,"+
//        s" integritylogo string ,"+
//        s" istop tinyint  ,"+
//        s" refcount int  ,"+
//        s" matupdateon timestamp  comment '供应商报价更新时间(指当前供应商下的报价全部已更新所对应的时间)',"+
//        s" publishdate timestamp  comment '发布日期',"+
//        s" isflagship string ,"+
//        s" ishonourable string ,"+
//        s" isjournal tinyint ,"+
//        s" association string,"+
//        s" stdnames string,"+
//        s" memberid string  comment '用户账号',"+
//        s" regprovince string comment '注册省份',"+
//        s" regcity string comment '注册城市',"+
//        s" iscertified tinyint   comment '是否认证(0:否，1：是，2：认证不通过)',"+
//        s" creationdate string  comment '成立日期',"+
//        s" refreshmatflag tinyint   comment '是否可以一键刷新，一天一次（1：是，0：否）',"+
//        s" mainproducttext string comment '主营产品',"+
//        s" isvip tinyint   comment '是否入库是否入库（0：待审核，1：审核通过，2：审核不通过）',"+
//        s" viptime timestamp  comment '入库通过时间',"+
//        s" certifiedtime timestamp  comment '认证通过时间',"+
//        s" fromtype string  comment '来源',"+
//        s" registerip string  comment '注册ip',"+
//        s" registeraddr string  comment '注册ip',"+
//        s" source tinyint   comment '1:造价通，2:gcw',"+
//        s" projectviewnum int  comment '工程查看数量',"+
//        s" guideflag int  comment '是否显示引导,0:引导，1：不引导',"+
//        s" istest tinyint  comment '是否测试账号(0：否，1：是)',"+
//        s" regcapitalunit string  comment '注册资金单位',"+
//        s" mail string  comment '邮箱',"+
//        s" upgradetips tinyint  comment '升级提示：1：提示，0：不提示',"+
//        s" bid string comment '大类',"+
//        s" regcapitalrmb decimal(14,2) comment '注册人民币，万元',"+
//        s" usertype string   comment '用户类型,例如供应商：gys',"+
//        s" levelcode string  comment '供应商等级',"+
//        s" projectviewprovince string comment '工程信息查看省份',"+
//        s" creditcode string  comment '社会统一信用代码',"+
//        s" isenvironmental tinyint   comment '是否环保企业(0:不是，1：是)',"+
//        s" qq string  comment '客服qq',"+
//        s" synthesisscore int  comment '企业综合得分',"+
//        s" tmpstatus int   comment '临时状态' "+
//        s") stored as parquet")

    /**
      * 2.连接mysql 数据库，并查询mysql数据库表中的数据
      */
    val etl_dt = "2018-10-15"
    var where_sql =s" where 1= 1 and DATE_FORMAT(COALESCE(UpdateOn,CreateOn),'%Y-%m-%d') ='$etl_dt'"
    val query_tEpShop_sql=s"(SELECT ID,EID,NAME,ADDR,POSTCODE,HOMEPAGE,CORPN,AREA,LOGO,LONGLOGO "+
      s" ,ADURL,CULTURE,BGLOGO,LINKMAN,MAINMATERIAL,SALEAREA,BRANCHINFO,PROVSORT " +
      s",PROVINCE,CITY,PICPATH,SQSPATH,BRAND,DISCRIPTION,REGAREA,REGADDRESS "+
      s",REGCAPITAL,REGTIME,REGOFFICE,STARTTIME,ENDTIME,REGNUM,BUSINESSSCOPE " +
      s",BUSINESSLICENSEPIC,BUSINESSMODE,ENTERPRISETYPE,SHOPTYPE,CREATEON,CREATEBY " +
      s",UPDATEON,UPDATEBY,ISLOCK,ISAUDIT,ISDELETED,AUDITOR,EFFECTDATE,AUDITDATE " +
      s",VALIDDATE,ISVOLID,MANAGEID,DEGREE,CID,SUBCID,CATALOGID,GRADE,HASUNAUDITMAT " +
      s",MATCOUNT,CONTACT,SEX,DEPARTMENT,MOBILE,PHONE,ISINTEGRITY,CREDITSCORE,AUTHCONTENT " +
      s",RECOMMENDCONTENT,PUTCOUNT,CERTIFNUM,FAX,INTEGRITYLOGO,ISTOP,REFCOUNT,MATUPDATEON " +
      s",PUBLISHDATE,ISFLAGSHIP,ISHONOURABLE,ISJOURNAL,ASSOCIATION,STDNAMES,MEMBERID " +
      s",REGPROVINCE,REGCITY,ISCERTIFIED,DATE_FORMAT(CREATIONDATE,'YYYY-MM-DD') as CREATIONDATE " +
      s",REFRESHMATFLAG,MAINPRODUCTTEXT,ISVIP,VIPTIME,CERTIFIEDTIME,FROMTYPE,REGISTERIP "+
      s",REGISTERADDR,SOURCE,PROJECTVIEWNUM,GUIDEFLAG,ISTEST,REGCAPITALUNIT,MAIL " +
      s",UPGRADETIPS,BID,REGCAPITALRMB,USERTYPE,LEVELCODE,PROJECTVIEWPROVINCE " +
      s",CREDITCODE,ISENVIRONMENTAL,QQ,SYNTHESISSCORE,TMPSTATUS from costexpressdb.tEpShop " +
      s" $where_sql ) as tEpShop "
    System.out.print(query_tEpShop_sql)
    val url =edw_config.properties.getProperty("mysql_edw.url")
    val user=edw_config.properties.getProperty("mysql_edw.user")
    val password=edw_config.properties.getProperty("mysql_edw.password")
    val tepshopDF = spark.read
        .format("jdbc")
        .option("url","jdbc:mysql://master:3306/costexpressdb")
        .option("user","root")
        .option("password","zjpl123")
        .option("dbtable",query_tEpShop_sql)
        .option("quoteAll",true)
        .load()
    tepshopDF.write.mode(SaveMode.Overwrite).saveAsTable("odm_db.o_tepshop")
    spark.stop()
  }
}
