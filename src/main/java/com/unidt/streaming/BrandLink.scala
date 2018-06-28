package com.unidt.streaming

import java.util

import com.unidt.helper.FraHelper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document

/**
  * Created by admin on 2018/4/4.
  */

case class Link(mac: String, date_time: String, from_name: String, to_name: String, times: Int) extends Serializable{

}

/**
  * 计算品牌动线关联数据
  */
object BrandLink {

  /**
    *
    * @param hiveContext
    */
  def safeGetBrandLink(hiveContext: HiveContext): Unit = {
    try{
      getBrandLink(hiveContext)
    }catch {
      case ex: Exception => {
        println(ex)
      }
    }
  }

  /**
    * 品牌动线关联应该按月统计, 所有按月统计的数据，统一放入pyspark中
    * @param hiveContext
    */
  def getBrandLink(hiveContext: HiveContext, p_dt:String  = ""): Unit = {
    import hiveContext.implicits._
    var start = System.currentTimeMillis()

    var sql = ""
    if (p_dt == "") {
      print("未指定分区，计算品牌关联时认为是实时计算当天数据")
      sql = "select mac, name, date_time from tmp_dw_andatong where name is not null order by mac, date_time"
    } else {
      sql = f"select mac, name, date_time from dw_andatong where name is not null and p_dt='$p_dt' order by mac, date_time"
    }


    println("品牌动线关联统计")
    val df = hiveContext.sql(sql).mapPartitions( iter => {
      var res = List[Link]()
      var pre: Row = null
      if (iter.hasNext){
        pre = iter.next()
      }
      while( pre != null && iter.hasNext ){
        var cur = iter.next()
        //
        // pre 为from， cur 为 to
        var mac_pre = pre(0).toString
        var mac_cur = cur(0).toString
        //
        // 如果两个 mac 一致， 统计其路径
        if (mac_pre == mac_cur) {
          var from_name = pre(1).toString
          var to_name = cur(1).toString
          var date_time = cur(2).toString
          var nrow = Link(mac_cur, date_time, from_name, to_name,1)
          if (from_name.compareToIgnoreCase(to_name) != 0){
            res.::= (nrow)
          }
        }
      }
      res.iterator
    }).map( r => r).toDF()
    df.repartition(1).createOrReplaceTempView("tmp_brand_link")
    var date = FraHelper.getDate()
    //
    // 存储品牌动线关联，方便日后分析
    // 因为每次都是基于全量数据进行的分析，所以采用 overwrite
    if (p_dt == "") {
      sql = f"insert overwrite table dm_brand_link partition(p_dt) select *, $date%s as p_dt from tmp_brand_link"
    } else {
      println(f"使用指定日期做分区: $p_dt")
      sql = f"insert overwrite table dm_brand_link partition(p_dt) select *, $p_dt%s as p_dt from tmp_brand_link"
    }

    hiveContext.sql(sql)
//    hiveContext.cacheTable("dm_brand_link")
    //
    // 写入Mongodb
    val tools = AndatongStreaming.TOOLS
    var docs = new util.ArrayList[Document]()
//    var start = FraHelper.getYear() + FraHelper.getMonth() + "01"
//    var end = FraHelper.getYear() + FraHelper.getMonth() + "31"
    sql = f"select a.from_name, a.to_name, times, total from(select from_name, to_name, count(*) as times from dm_brand_link " +
      f"where p_dt='$date' group by from_name, to_name)a left join (select from_name, count(*) as total from dm_brand_link " +
      f"where p_dt='$date' group by from_name)b on a.from_name = b.from_name"
//    hiveContext.sql(sql).collect().foreach(r => {
//      val from_name = r(0).toString
//      val to_name = r(1).toString
//      var doc = tools.init_document("sh001", "brand_link")
//      var data = new util.HashMap[String, String]()
//      data.put("from_name", from_name)
//      data.put("to_name", to_name)
//      data.put("times", r(2).toString)
//      data.put("total", r(3).toString)
//      doc = doc.append("bi_value", data)
//      docs.add(doc)
//    })
//    println("写入MongoDB")
//    tools.deleteMany("brand_link")
//    tools.insertMany(docs)
    var end = System.currentTimeMillis()
    var total = end - start
    println(f"计算品牌关联耗时: $total")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("brand-link").config("hive.exec.dynamic.partition.mode", "nonstrict")
                .enableHiveSupport().getOrCreate()
    val hiveContext = new HiveContext(spark.sparkContext)
    hiveContext.sql("use andatong")
    hiveContext.sql("set hive.mapred.supports.subdirectories=true")
    hiveContext.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
    hiveContext.sql("set hive.exec.dynamic.partition=true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180428")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180429")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180430")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180501")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180502")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180503")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180504")
    BrandLink.getBrandLink(hiveContext, p_dt = "20180505")
  }
}
