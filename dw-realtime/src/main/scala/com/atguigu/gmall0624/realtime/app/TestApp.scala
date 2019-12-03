package com.atguigu.gmall0624.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.bean.{EventInfo, OrderInfo}
import com.atguigu.gmall0624.realtime.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestApp")
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
		val inputStreamDs: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

		val orderInfoDstream: DStream[OrderInfo] = inputStreamDs.map {
			recored =>
				//获取里面的值
				val jsonString: String = recored.value()
				//利用样例类将数据转换成对象
				val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
				//将电话进行切分进行脱敏的操作
				val tuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
				val telTail: String = tuple._2.splitAt(4)._2
				orderInfo.consignee_tel = tuple._1 + "****" + telTail
				//改变日期的结构

				val strings: Array[String] = orderInfo.create_time.split(" ")
				orderInfo.create_date = strings(0)
				orderInfo.create_hour = strings(1).split(":")(0)
				orderInfo
		}
		orderInfoDstream.foreachRDD(rdd=>{
			println(rdd.collect().mkString("\n"))
		})
		orderInfoDstream.print()
		ssc.start()
		ssc.awaitTermination()
	}
}
