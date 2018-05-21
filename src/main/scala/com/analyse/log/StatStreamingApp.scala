package com.analyse.log

import com.analyse.domain._
import com.analyse.project.Data
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object StatStreamingApp {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "StatStreamingApp", Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "s01:9092,s02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    <!-- val topics = List("flumeTopic").toSet -->
    val topics = Array("flumeTopic")
    val logs = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value())

   // logs.print()

    //187.167.100.29	2018-05-16 16:31:31	"GET www/6 HTTP/1.0"	-	302
    var cleanLog = logs.map(line=>{
      var infos = line.split("\t")
      var url = infos(2).split(" ")(1)
      var categaryId = 0
      if(url.startsWith("www")){
        categaryId = url.split("/")(1).toInt
      }
      ClickLog(infos(0),Data.parseToMin(infos(1)),categaryId,infos(3),infos(4).toInt)
    }).filter(log=>log.categaryId!=0)

    cleanLog.print()

    //每个类别的每天的点击量 (day_categaryId,1)
    cleanLog.map(log=>{
      (log.time.substring(0,8)+log.categaryId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition( partitions=>{
        val list = new ListBuffer[CategaryClickCount]
        partitions.foreach(pair=>{
          list.append(CategaryClickCount(pair._1,pair._2))
        })
        LogDao.save(list)
      })
    })



    //每个栏目下面从渠道过来的流量20180517_www.baidu.com_1 100 20171122_2（渠道）_1（类别） 100
    //categary_search_count   create "search_log_count","info"
    //124.30.187.10	2018-05-17 00:39:26	"GET www/6 HTTP/1.0"
    // 	https:/www.sogou.com/web?qu=我的体育老师	302
    cleanLog.map(log=>{
      val url = log.refer.replace("//","/")
      val splits =   url.split("/")
      var host =""
      if(splits.length > 2){
        host=splits(1)
      }
      (host,log.time,log.categaryId)
    }).filter(x=>x._1 != "").map(x=>{
      (x._2.substring(0,8)+"_"+x._1+"_"+x._3,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partions=>{
        val list = new ListBuffer[CategarySearchClickCount]
        partions.foreach(pairs=>{
          list.append(CategarySearchClickCount(pairs._1,pairs._2))
        })
        SearchDao.save(list)
      })
    })



    ssc.start();
    ssc.awaitTermination();
  }
}
