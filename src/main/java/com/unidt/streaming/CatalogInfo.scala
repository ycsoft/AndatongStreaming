package com.unidt.streaming

import java.util

import com.mongodb.client.model.Filters
import com.unidt.helper.FraHelper
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document

/**
  * Created by admin on 2018/4/3.
  */
object CatalogInfo {

  def safeGetCatalogInfo(hiveContext: HiveContext): Unit = {
    try{
      getCatalogInfo(hiveContext)
    }catch {
      case ex: Exception=>{
        ex.printStackTrace()
      }
    }
  }
  def getCatalogInfo(hiveContext: HiveContext): Unit = {
    var start = System.currentTimeMillis()
    val dw_table = "tmp_dw_andatong"
    val rt_table = "dw_rt_andatong"

    //
    // 今日全场人气最热商铺品类
    val tools = AndatongStreaming.TOOLS
    var docs = new util.ArrayList[Document]()

    println("今日全场人气最热商铺品类")
    var sql = f"select type, count(distinct mac) as total from $dw_table where type is not null and name != '走道' group by type order by total desc limit 10"
    hiveContext.sql(sql).collect().foreach( row => {
      val tp = row(0)
      val total = row(1)
      println(f"$tp\t$total")
      var doc = tools.init_document("sh001", "day_catalog")

      var data  = new util.HashMap[String, String]()
      data.put("catalog", tp.toString)
      data.put("total", total.toString)
      doc = doc.append("bi_value", data)
      docs.add(doc)
    })
    //删除旧数据
    tools.deleteMany("day_catalog")
    tools.insertMany(docs)
    docs.clear()

    println("实时全场最热品类")
    //
    // 实时数据参考意义并不大，改为过去1小时内的数据
    var hour = (FraHelper.getHour().toInt - 1).toString
//    sql = f"select if(type is null, '过道', type), count(mac) as total from $dw_table where dt_hour> '$hour' group by type order by total desc limit 10"
    sql = f"select if(type is null, '走道', type), count(mac) as total from $rt_table where name!='走道' group by type order by total desc limit 10"
    print(sql)
    hiveContext.sql(sql).collect().foreach(row => {
      val tp = row(0)
      val total = row(1)
      println(f"$tp\t$total")
      var doc = tools.init_document("sh001", "rt_catalog")
      var data  = new util.HashMap[String, String]()
      data.put("catalog", tp.toString)
      data.put("total", total.toString)
      doc = doc.append("bi_value", data)
      docs.add(doc)
    })
    tools.deleteMany("rt_catalog")
    tools.insertMany(docs)
    docs.clear()

    println("Done")
    var end = System.currentTimeMillis()
    var total = end - start
    println(f"最热商品品类统计耗时: $total")
  }

}
