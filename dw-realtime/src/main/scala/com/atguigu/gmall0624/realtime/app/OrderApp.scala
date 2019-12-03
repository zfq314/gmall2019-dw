package com.atguigu.gmall0624.realtime.app


import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.bean.OrderInfo
import com.atguigu.gmall0624.realtime.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.phoenix.spark._

object OrderApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
		//消费kafka的数据
		val inputStramDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
		val orderInfoStream: DStream[OrderInfo] = inputStramDS.map {
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
		//对数据进行简单的操作处理

		orderInfoStream.foreachRDD{
			rdd =>
				val configuration = new Configuration()
				println(rdd.collect().mkString("\n"))
				rdd.saveToPhoenix("GMALL0624_ORDER_INFO", Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), configuration, Some("hadoop102,hadoop103,hadoop104:2181"))

		}

		ssc.start()
		ssc.awaitTermination()
	}
}
