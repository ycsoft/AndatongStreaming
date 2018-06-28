package com.unidt.streaming

import java.util
import com.mongodb.client.model.Filters
import com.unidt.helper.FraHelper
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document

/**
  * Created by admin on 2018/4/3.
  */
object FloorFlow {

  /**
    *
    * @param hiveContext
    */
  def safeGetFloorFlow(hiveContext: HiveContext): Unit = {
    try{
      getFloorFlow(hiveContext)
    }catch {
      case ex: Exception=>{
        ex.printStackTrace()
      }
    }
  }
  /**
    * 获取本层今日客流
    */
  def getFloorFlow(hiveContext: HiveContext): Unit = {
    var start = System.currentTimeMillis()
    val tools = AndatongStreaming.TOOLS
    var docs = new util.ArrayList[Document]()
    var hour = FraHelper.getHour().toInt

    var intmin = FraHelper.getMinute().toInt - 10
    if (intmin <= 0){
      intmin = 50
      hour = FraHelper.getHour().toInt - 1
    }

    var sql = "select locName, dt_hour, dt_min, count(distinct *) from tmp_dw_andatong group by locName, dt_hour, dt_min order by locName, dt_hour, dt_min"

    hiveContext.sql(sql).collect().foreach( row => {
      var floor = row(0)
      var count = row(3)
      //
      // TODO: Insert data to MongoDB
      val date = FraHelper.getDate()
      var doc = tools.init_document("sh001", "floor_day_flow",date, row(1).toString, row(2).toString)
      var data  = new util.HashMap[String, String]()

      data.put("floor", floor.toString)
      data.put("count", count.toString)
      doc = doc.append("bi_value", data)
      docs.add(doc)
    })
    tools.deleteMany( "floor_day_flow")
    tools.insertMany(docs)

    var end = System.currentTimeMillis()
    var total = end - start
    println(f"单楼层信息统计耗时: $total")
    //
    // 生成每楼层热点图
    println("生成楼层热力图")
    docs.clear()
    start = System.currentTimeMillis()
    sql = "select locName, posX, posY, count(distinct mac) from tmp_dw_andatong group by locName, posX, posY"
    hiveContext.sql(sql).collect().foreach( row => {
      val floor = row(0).toString
      val posx = row(1).toString.toInt
      val posy = row(2).toString.toInt
      val count = row(3).toString.toInt

      var date = FraHelper.getDate()
      var doc = tools.init_document("sh001", "heat_map",date, FraHelper.getHour(), FraHelper.getMinute())

//      var value = new util.HashMap[String, String]()
//      value.put("floor", floor)
//      value.put("x", posx.toString)
//      value.put("y", posy.toString)
//      value.put("count", count.toString)
      //
      // 小于 5 人的不入库
      if (count > 5) {
        var value = new Document("floor", floor)
          .append("x", posx)
          .append("y", posy)
          .append("count", count)
        doc.put("bi_value", value)
        docs.add(doc)
      }
    })
    end = System.currentTimeMillis()
    total = end - start
    print(f"计算热力数据耗时: $total")
    start = System.currentTimeMillis()
    tools.deleteMany("heat_map")
    tools.insertMany(docs)
    end = System.currentTimeMillis()
    total = end - start
    println(f"写入热力数据耗时: $total")
  }
}
