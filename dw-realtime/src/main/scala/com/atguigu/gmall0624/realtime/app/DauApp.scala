package com.atguigu.gmall0624.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.bean.StartUpLog
import com.atguigu.gmall0624.realtime.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf

object DauApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")

		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
		val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)
		//转换格式 同时补充两个时间字段
		val startlogDStream: DStream[StartUpLog] = inputDstream.map { record =>
			val jsonString: String = record.value()
			val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
			val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
			val dateHour: String = formatter.format(new Date(startUpLog.ts))
			val dataHourArr: Array[String] = dateHour.split(" ")
			startUpLog.logDate = dataHourArr(0)
			startUpLog.logHour = dataHourArr(1)
			startUpLog
		}

		//各个批次见去重
		val filterDStream: DStream[StartUpLog] = startlogDStream.transform { rdd =>
			println("过滤前：" + rdd.count())
			//driver 周期性的查询redis清单 通过广播变量发送到executor
			val jedis = new Jedis("hadoop102", 6379)
			val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
			val dateKey: String = "dau:" + dateStr
			val dauMidSet: util.Set[String] = jedis.smembers(dateKey)
			jedis.close()
			val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
			//根据广播变量比对自己的数据，进行过滤
			val filterRDD: RDD[StartUpLog] = rdd.filter { startupLog =>
				val midSet: util.Set[String] = dauMidBC.value
				!midSet.contains(startupLog.mid)
			}
			println("过滤后：" + filterRDD.count())
			filterRDD
		}
		//但是同一批次能也有数据的重复,用mid去重，去取第一条数据
		val startGroupbyMiDstream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(startuplog => (startuplog.mid, startuplog)).groupByKey()
		val filstrDstream2: DStream[StartUpLog] = startGroupbyMiDstream.flatMap { case (mid, startuplogIter) =>
			val sortList: List[StartUpLog] = startuplogIter.toList.sortWith((strartuplog1, strartuplog2) =>
				strartuplog1.ts < strartuplog2.ts)
			val top1LogList: List[StartUpLog] = sortList.take(1)
			top1LogList
		}
		filstrDstream2.cache()
		//将数据保存到redis中
		filstrDstream2.foreachRDD { rdd =>
			//数据过滤 drive执行
			rdd.foreachPartition { startupLogIter =>
				val jedis: Jedis = new Jedis("hadoop102", 6379)
				for (strartuplog <- startupLogIter) {
					println(strartuplog)
					val dateKey: String = "dau:" + strartuplog.logDate
					jedis.sadd(dateKey, strartuplog.mid)
				}
				jedis.close()
			}
		}
		//ctrl +shift +u 切换大小写
		filstrDstream2.foreachRDD { rdd =>
			if(rdd.count()>0){
				rdd.saveToPhoenix("GMALL0624_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
	     	}
		 }
		ssc.start()
		ssc.awaitTermination()
	}
}
