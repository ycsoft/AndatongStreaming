package com.unidt.streaming

import java.util
import java.util.UUID

import com.unidt.helper.FraHelper
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import java.util.HashMap
import java.util.ArrayList

import com.mongodb.client.model.Filters


/**
  * Created by admin on 2018/3/23.
  */
case class  AndatongPos( shopcode: String
                         ,date_time:String
                       , dt_year: Int
                       , dt_month: Int
                       , dt_day: Int
                       , dt_hour: Int
                       , dt_min: Int
                       , dt_sec: Int
                       , time_stamp: Int
                         , mac: String
                         , ipAddr: String
                         , locID: String
                         , locName: String
                         , posX: Int
                         , posY: Int) extends java.io.Serializable {
}

object AndatongStreaming {

  var TOOLS = FraHelper.getMongoDB(FraHelper.Mongo_DB, FraHelper.Mongo_BI)

  def flushData(): Unit = {
    var conf = new SparkConf().setAppName("andatong-Kafka-streaming")
    var sparkContext = new SparkContext(conf)
    var ssc = new StreamingContext(sparkContext, Seconds(60))

    val topics = Set("andatong")
    val kafkaParams = Map[String, String]("bootstrap.servers" -> "192.168.0.14:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "false"
    )
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val spark = SparkSession.builder().config(conf)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val rdds = message.map(line => {
      val array = line._2.split(",")
      array
    }).foreachRDD( rd => {
      val newrdd = rd.map(attr => AndatongPos(attr(0)
        , attr(1)
        , FraHelper.getYear(attr(1))
        , FraHelper.getMonth(attr(1))
        , FraHelper.getDay(attr(1))
        , FraHelper.getHour(attr(1))
        , FraHelper.getMinute(attr(1))
        , FraHelper.getSeconds(attr(1))
        , FraHelper.get_time_stamp(attr(1))
        ,attr(2)
        ,attr(3)
        ,attr(4)
        ,attr(5)
        ,Math.floor(attr(6).trim.toInt/13.1).toInt
        ,Math.floor(attr(7).trim.toInt/13.1).toInt))

      //
      // 每次生成新的目录
      val datepartition = FraHelper.getDate()
      val hiveContext = new HiveContext(spark.sparkContext)
      import hiveContext.implicits._

      hiveContext.sql("use andatong")
      hiveContext.sql("set hive.mapred.supports.subdirectories=true")
      hiveContext.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
      hiveContext.sql("set hive.exec.dynamic.partition=true")
      hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      // hiveContext.sql("set hive.exec.stagingdir=/tmp/hive/hadoop/andatong")

      val df = newrdd.toDF()
      if (df.count() > 0){
        var istart = System.currentTimeMillis()
        df.repartition(1).createOrReplaceTempView("ods_rt_andatong")
        hiveContext.sql("select count(*) from ods_rt_andatong").collect().foreach(r => {
          var ct = r(0).toString
          println(f"实时数据条数: $ct")
        })
        //
        // 实时数据写入HDFS
        hiveContext.sql(f"insert into table ods_andatong partition(p_dt)" +
          f" select *, $datepartition%s as p_dt from ods_rt_andatong")
        var iend = System.currentTimeMillis()
        var itotal = iend - istart
        println(s"数据写入HDFS耗时： $itotal")
        val start = System.currentTimeMillis();
        //
        // 清洗实时数据并注册临时表
        // 去除黑名单mac
        val sql = f"SELECT a.shopcode, a.date_time, a.dt_year, " +
          f"a.dt_month, a.dt_day, a.dt_hour, a.dt_min, a.dt_sec, " +
          f"a.time_stamp, a.mac, a.ipAddr, a.locID, a.locName, " +
          f"a.posX, a.posY, b.name AS name, b.type as type FROM " +
          f"ods_rt_andatong a left JOIN dw_andatong_shop b " +
          f"ON a.posX = b.posx AND a.posY = b.posy and a.locName=b.floor " +
          f"left join ods_mac_inner c on a.mac=c.mac where c.mac is null"

//        val realDF = hiveContext.sql(f"SELECT a.shopcode, a.date_time, a.dt_year, a.dt_month, a.dt_day, a.dt_hour, " +
//          f"a.dt_min, a.dt_sec, a.time_stamp, a.mac, a.ipAddr, a.locID, a.locName, a.posX, a.posY, b.name AS name, " +
//          f"b.type as type FROM ods_rt_andatong a left JOIN dw_andatong_shop b ON a.posX = b.posx AND a.posY = b.posy and a.locName=b.floor")
        val realDF = hiveContext.sql(sql)
        realDF.createOrReplaceTempView("dw_rt_andatong")
        hiveContext.cacheTable("dw_rt_andatong")

        hiveContext.sql("select count(*) from dw_rt_andatong").collect().foreach( r => {
          val ct = r(0).toString
          println(f"清洗后的数据条数: $ct")
        })
        //
        // 数据清洗
        val dwdf = wash_data(hiveContext)
        val date = FraHelper.getDate()
        //
        // 清洗后的全量数据，注册为临时表 tmp_dw_andatong
        dwdf.createOrReplaceTempView("tmp_dw_andatong")
        hiveContext.cacheTable("tmp_dw_andatong")
        //
        //清洗完毕后的数据应该写入HDFS，方便日后的统计，否则需要重新清洗
        istart = System.currentTimeMillis()
        hiveContext.sql(f"insert overwrite table dw_andatong partition(p_dt) select *, $date%s as p_dt from tmp_dw_andatong")
        //dwdf.repartition(1).write.mode(SaveMode.Overwrite).save(f"/user/hadoop/andatong/dw/kafka/p_dt=$date")
        iend = System.currentTimeMillis()
        itotal = iend - istart
        print(f"写入DW耗时: $itotal 毫秒")
        //
        // 统计
        try{
          safe_customer_flow(hiveContext, dwdf)
          safe_flow_max_shop(hiveContext, dwdf, realDF)
          CatalogInfo.safeGetCatalogInfo(hiveContext)
          BrandLink.safeGetBrandLink(hiveContext)
          FloorFlow.safeGetFloorFlow(hiveContext)
        }catch {
          case ex: Exception => {
            println(ex)
          }
        }
        hiveContext.clearCache()
        val end = System.currentTimeMillis()
        val time_cost = end - start
        println(f"总耗时: $time_cost")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * 数据清洗
    * 清除非营业时间的数据点\持续时间超过4小时的点
    * 关联店铺名称，以便分析店铺客流等指标
    * @param hiveContext
    */
  def wash_data(hiveContext: HiveContext): DataFrame = {
    val start = System.currentTimeMillis()
    val date = FraHelper.getDate()
    println(f"清洗全量数据: $date")
//    var sql = f" SELECT a.shopcode, a.date_time, a.dt_year, a.dt_month, a.dt_day, a.dt_hour, " +
//      f"a.dt_min, a.dt_sec, a.time_stamp, a.mac, a.ipAddr, a.locID, a.locName, a.posX, a.posY, " +
//      f"b.name AS name, b.type as type FROM( SELECT * FROM ods_andatong WHERE p_dt='$date%s' AND dt_hour >= 9 AND dt_hour < 23)a " +
//      f" left JOIN dw_andatong_shop b ON a.posX = b.posx AND a.posY = b.posy  and a.locName=b.floor"

    val sql = f"SELECT a.shopcode, a.date_time, a.dt_year, a.dt_month, " +
      f"a.dt_day, a.dt_hour, a.dt_min, a.dt_sec, a.time_stamp, a.mac, " +
      f"a.ipAddr, a.locID, a.locName, a.posX, a.posY, b.name AS name, " +
      f"b.type as type FROM( SELECT * FROM ods_andatong WHERE p_dt='$date' " +
      f"AND dt_hour >= 9 AND dt_hour < 23)a left JOIN dw_andatong_shop b " +
      f"ON a.posX = b.posx AND a.posY = b.posy and a.locName = b.floor " +
      f"left join ods_mac_inner c on a.mac = c.mac where c.mac is null"

    val df = hiveContext.sql(sql)
    val end = System.currentTimeMillis()
    val total = end - start
    println(f"数据清洗耗时: $total")
    df
  }

  /**
    *  每60秒统计一次实时客流
    *  全量统计547万条数据的话，该指标耗时 4 秒左右
    *  最坏情况：  4 s
    * @param hiveContext
    */
  def customer_flow(hiveContext: HiveContext, dwdf: DataFrame): Unit = {
    val start = System.currentTimeMillis()
    val tempTable = "tmp_dw_andatong"
    val rtTable = "dw_rt_andatong"

    val datetime = FraHelper.getDate()
//    //
//    // 实时数据，过去10分钟的数据
//    var dt_min = FraHelper.getMinute().toInt - 10
//    var dt_hour = FraHelper.getHour().toInt
//    if (dt_min < 0) {
//      dt_min = 50
//      dt_hour = dt_hour - 1
//    }
    //
    // 按时间进行过滤貌似并不能解节省多少时间
    var sql_rt = f"select dt_hour, dt_min, all_flow, all_customer,stay_time/total_mac/60.0 from" +
      f"( select dt_hour, dt_min, count(mac) as all_flow, count(distinct mac) as all_customer from $rtTable%s group by dt_hour, dt_min)a " +
      f"left join ( select sum(tm_end - tm_begin) as stay_time, count(distinct mac) as total_mac from ( select mac, max(time_stamp) as tm_end , min(time_stamp) as tm_begin " +
      f"from $tempTable%s group by mac)t" +
      f" )b order by dt_hour, dt_min"
    //
    // 实时客流
    println("Real Time Customer Flow")
    println(sql_rt)
    val tools = TOOLS
    var ret_doc = new Document("bi_name","today_flow")
    val docs = new java.util.ArrayList[Document]()
    hiveContext.sql(sql_rt).repartition().collect().foreach(row => {
      var hour = row(0)
      var min = row(1)
      var all_flow = row(2)
      var all_customer = row(3)
      var stay = row(4)
      println(f"Real Time: $all_customer, AVG Stay: $stay")
      //
      // 插入数据库
      //TODO: Insert data into MongoDB
      var doc = tools.init_document("sh001", "today_flow")
      var data = new java.util.HashMap[String, String]()

      data.put("count", all_customer.toString)
      data.put("staty", stay.toString)
      doc = doc.append("bi_value", data)
      docs.add(doc)
    })
    ret_doc = ret_doc.append("data", docs)
    //
    tools.insertMany(docs)
    docs.clear()
    //
    // 统计今日客流总数,商场内平均停留时间
    println("Today Customer Flow")
    var total: String = ""
    var stay: String = ""
    var sql_all = f"select count(distinct mac) from $tempTable%s" // 统计今日总人数

    hiveContext.sql(sql_all).collect().foreach(row => {
      var all = row(0)
      total = all.toString
      println(f"Today Total: $all")
      total = all.toString
    })

    var sql_stay = f"select sum(stay) as all_stay_time from(select mac, (max(time_stamp) - min(time_stamp)) as stay from $tempTable%s group by mac)t "
    hiveContext.sql(sql_stay).collect().foreach( row => {
      stay = row(0).toString
      println(f"Total Stay: $stay")
    })
    val end = System.currentTimeMillis()
    val time_cost = end - start
    var doc = tools.init_document("sh001", "today_total")
    var data = new util.HashMap[String, String]()

    val avg_stay = stay.toInt / total.toInt / 60.0
    data.put("all_customer", total)
    data.put("all_stay", avg_stay.toString)
    doc = doc.append("bi_value", data)
    docs.add(doc)
//    tools.mongo_collection.deleteMany(Filters.eq("bi_name", "today_total"))
    tools.deleteMany("today_total")
    tools.insertMany(docs)
    println(f"今日客流统计耗时： $time_cost")
  }

  def safe_customer_flow(hiveContext: HiveContext, dwdf: DataFrame): Unit = {
    try {
      customer_flow(hiveContext, dwdf)
    } catch {
      case ex: Exception => {
        println(ex)
      }
    }
  }


  /**
    * 计算实时全场客流最大商铺
    * 今日全场客流最大商铺
    * @param hiveContext
    * @param dwdf 全量数据
    * @param realTimeDF 实时数据
    */
  def flow_max_shop(hiveContext: HiveContext, dwdf: DataFrame, realTimeDF: DataFrame): Unit = {
    val start = System.currentTimeMillis()
    val tempTable = "tmp_dw_andatong"
    val rtTable = "dw_rt_andatong"
    //
    // 实时全场客流最大商铺
    // 由于过去1分钟内数据实在太少，改为从dw库中查询，改为过去10分钟内的数据
    var hour = FraHelper.getHour().toInt
    var intmin = FraHelper.getMinute().toInt - 10
    if (intmin <= 0){
      intmin = 50
      hour = FraHelper.getHour().toInt - 1
    }

    hour = FraHelper.getHour().toInt
    intmin = FraHelper.getMinute().toInt
    //var sql= f"select name, count(distinct mac) as mac_all from $tempTable%s where dt_hour>='$hour%d' and dt_min>='$intmin%d' group by name order by mac_all desc limit 10"
    var sql = f"select if(name is null, '走道', name), count(distinct mac) as mac_all from $rtTable%s where name != '走道' group by name order by mac_all desc limit 10"
    println("实时全场客流最大商铺")
    println(sql)
    val tools = TOOLS
    val docs = new java.util.ArrayList[Document]()
    hiveContext.sql(sql).collect().foreach(r =>{
      //
      // 将实时商铺客流写入数据库
      // TODO: Insert data into MongoDB
      var shop = r(0)
      var flow = r(1)
      println(f"Shop: $shop, Flow: $flow")
      //实时全场客流最大商铺
      var doc = tools.init_document("sh001", "rt_max_shop")
      var data = new util.HashMap[String, String]()
      data.put("shop", shop.toString)
      data.put("flow", flow.toString)
      doc = doc.append("bi_value", data)
      docs.add(doc)
    })
    //
    // 删除旧数据
    tools.deleteMany("rt_max_shop")
    tools.insertMany(docs)
    docs.clear()
    //
    // 统计今日全场客流最大商铺
    val date = FraHelper.getDate()
    sql = f"select if(name is null, '走道', name), count(distinct mac) as mac_all from $tempTable%s where name != '走道'  group by name order by mac_all desc"
    println("今日全场客流最大商铺")
    hiveContext.sql(sql).collect().foreach( r => {
      //
      // TODO: Insert data into MongoDB
      var shop = r(0)
      var flow = r(1)
      println(f"Shop: $shop, Flow: $flow")
      var doc = tools.init_document("sh001", "day_max_shop")
      var data = new util.HashMap[String, String]()
      data.put("shop", shop.toString)
      data.put("flow", flow.toString)
      doc = doc.append("bi_value", data)
      docs.add(doc)
    })
    //
    // 删除旧数据
    //tools.mongo_collection.deleteMany(Filters.eq("bi_name", "day_max_shop"))
    tools.deleteMany("day_max_shop")
    tools.insertMany(docs)
    docs.clear()
    //
    // 实时楼层人数, 实时数据太少，修改为统计过去一小时的数据
//    sql = f"select a.locName, uv, total_stay/total_mac, name from" +
//      f"(select locName, count(distinct mac) as uv from tmp_dw_andatong where dt_hour>='$hour' group by locName)a left join " +
//      f"(select locName, count(distinct mac) as total_mac, sum(stay) as total_stay from " +
//      f"(select locName, mac , (max(time_stamp)-min(time_stamp)) as stay from tmp_dw_andatong where dt_hour>='$hour' group by locName, mac)t1 group by locName)b " +
//      f"on a.locName = b.locName " +
//      f"left join ( select locName, name, count(distinct mac) as flow from tmp_dw_andatong where dt_hour>='$hour' group by locName, name order by flow desc limit 3)c " +
//      f"on b.locName = c.locName"
//    sql = f"select a.locName, uv, total_stay/60.0/total_mac, if(name is null, '走道', name) as name from" +
//      f"(select locName, count(distinct mac) as uv from $rtTable group by locName)a left join " +
//      f"(select locName, count(distinct mac) as total_mac, sum(stay) as total_stay from " +
//      f"(select locName, mac , (max(time_stamp)-min(time_stamp)) as stay from $tempTable  group by locName, mac)t1 group by locName)b " +
//      f"on a.locName = b.locName " +
//      f"left join ( select locName, name, count(distinct mac) as flow from $tempTable where name != '走道' and name!='过道'  group by locName, name order by flow desc limit 3)c " +
//      f"on b.locName = c.locName"
    sql = f"SELECT a.locName, uv, total_stay/60.0/total_mac, if(name is null, '走道', name) as name, flow " +
      f"from(SELECT locName, count(distinct mac) AS uv FROM $rtTable GROUP BY locName)a " +
      f"LEFT JOIN (SELECT locName, count(distinct mac) AS total_mac, sum(stay) AS total_stay " +
      f"FROM (SELECT locName, mac , (max(time_stamp)-min(time_stamp)) AS stay FROM $tempTable " +
      f"GROUP BY locName, mac)t1 GROUP BY locName)b ON a.locName = b.locName " +
      f"LEFT JOIN ( select locName, name, flow, " +
      f"row_number() over(partition by locName order by flow desc) as rank " +
      f"from (SELECT locName, name, count(distinct mac) AS flow " +
      f"FROM $tempTable WHERE name != '走道' and name != '过道' GROUP BY locName, name)t )c " +
      f"ON b.locName = c.locName where c.rank <= 3 order by locName, c.rank desc"
    println("实时楼层人数")
    print(sql)
    hiveContext.sql(sql).collect().foreach(r => {
      //
      // TODO: Insert data into MongoDB
      var floor = r(0)
      var uv = r(1)
      var stay = r(2)
      var shop = r(3)
      var flow = r(4)
      var doc = tools.init_document("sh001", "rt_floor_flow")
      var data = new util.HashMap[String, String]()

      data.put("floor", floor.toString)
      data.put("uv", uv.toString)
      data.put("stay", stay.toString)
      data.put("shop", shop.toString)
      data.put("flow", flow.toString)

      doc = doc.append("bi_value", data)
      docs.add(doc)
      println(f"Floor: $floor, UV: $uv, Stay: $stay, Shop: $shop")
    })
//    tools.mongo_collection.deleteMany(Filters.eq("bi_name", "rt_floor_flow"))
    tools.deleteMany("rt_floor_flow")
    tools.insertMany(docs)
    docs.clear()

    val end = System.currentTimeMillis()
    val time_cost = end - start
    println(f"商铺客流信息统计耗时: $time_cost")
  }

  def safe_flow_max_shop(hiveContext: HiveContext, dwdf: DataFrame, realTimeDF: DataFrame): Unit = {
    try {
      flow_max_shop(hiveContext, dwdf, realTimeDF)
    } catch {
      case ex: Exception => {
        println(ex)
      }
    }
  }
  def main(args: Array[String]): Unit = {
    flushData()
  }

}
