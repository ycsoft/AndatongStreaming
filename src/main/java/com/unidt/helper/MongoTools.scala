package com.unidt.helper

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import org.bson.Document

/**
  * Created by admin on 2018/4/8.
  */
class MongoTools {
  var mongo_client: MongoClient = null
  var mongo_collection: MongoCollection[Document] = null

  def insertOne(doc: Document): Unit = {
    try{
      mongo_collection.insertOne(doc)
    }catch {
      case  ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def insertMany(docs: java.util.List[Document]): Unit = {
    try{
      if (docs.size() > 0 )
        mongo_collection.insertMany(docs)
    }catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  /**
    * 删除当天的指定的BI数据
    * @param bi_name
    */
  def deleteMany(bi_name: String): Unit = {
    var date = FraHelper.getDate();
    mongo_collection.deleteMany(Filters.and(Filters.eq("bi_name", bi_name), Filters.eq("p_dt", date)))
  }

  def init_document(mall: String, bi_name: String): Document = {
    val datetime = FraHelper.getDate()
    var doc = new Document("mall_id", mall)
      .append("p_dt", datetime).append("dt_year", FraHelper.getYear()).append("dt_month", FraHelper.getMonth())
      .append("dt_day", FraHelper.getDay()).append("dt_hour", FraHelper.getHour()).append("dt_min", FraHelper.getMinute())
      .append("bi_name", bi_name)
    doc
  }

  def init_document(mall:String, bi_name: String, p_dt: String, hour: String, minute: String): Document = {
    var doc = new Document("mall_id", mall)
      .append("p_dt", p_dt).append("dt_year", FraHelper.getYear(p_dt).toString).append("dt_month", FraHelper.getMonth(p_dt).toString)
      .append("dt_day", FraHelper.getDay(p_dt).toString).append("dt_hour", hour).append("dt_min", minute)
      .append("bi_name", bi_name)
    doc
  }

  def close() : Unit = {
    mongo_client.close()
  }
}
