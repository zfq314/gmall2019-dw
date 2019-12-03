package com.atguigu.gmall0624.realtime.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0624.realtime.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

		val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)
		//对基本的数据数据进行转换
		inputDstream.print()
		val eventInfoDstream: DStream[EventInfo] = inputDstream.map { recored =>
			val jsonString: String = recored.value()
			//将字符串解析成指定的对象类型
			val info: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
			//对时间字段进行相应的处理

			//获取传过来对象的时间戳，然后做相应的转换
			val date: Date = new Date(info.ts);
			val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
			val dateString: String = dateFormat.format(date)

			val dateArrs: Array[String] = dateString.split(" ")
			info.logDate = dateArrs(0)
			info.logHour = dateArrs(1)
			info
		}
		//5分钟 开窗口
		val WindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300), Seconds(5))
		//分组，分组前 先进行map的转换，转换之后才能进行相应分组操作
		val groupMidDstream: DStream[(String, Iterable[EventInfo])] = WindowDstream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()
		groupMidDstream.cache()
		WindowDstream.foreachRDD(rdd=>
			println(rdd.collect().mkString("\n"))
		)
		// a 三次及以上用不同账号登录并领取优惠劵  得到窗口内 mid的操作行为集合  判断集合是否符合预警规则
		// b 并且在登录到领劵过程中没有浏览商品。
		val alertInfoDStream: DStream[(Boolean, AlertInfo)] = groupMidDstream.map { case (mid, eventInfoIter) =>
			var isAlert = false
			var isClickItem = false
			//引入java的集合，是为了将数据存储到es，如果引入scala的集合，没有办法转成json无法存放到es中
			var eventList = new util.ArrayList[String]()
			var uidSet = new util.HashSet[String]()
			var itemSet = new util.HashSet[String]()

			//对数据进行过滤
			breakable(
				for (eventInfo: EventInfo <- eventInfoIter) {
					if (eventInfo.evid == "coupon") {
						uidSet.add(eventInfo.uid)
						itemSet.add(eventInfo.itemid)
					}
					if (eventInfo.evid == "clickItem") {
						//如果有商品的点击，就不符合预警的条件，不要这样的数据
						isClickItem = true
						break
					}
					eventList.add(eventInfo.evid)
				}
			)
			// 购物车登录账号>3个  且 未点击商品信息
			if (uidSet.size() > 3 && isClickItem == false) {
				isAlert = true
			}
			(isAlert, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
		}

		alertInfoDStream.foreachRDD(rdd =>
			println(rdd.collect().mkString("\n"))
		)
		//只有数据是有预警信号才将数据存储
		val filteredAlertInfo: DStream[(Boolean, AlertInfo)] = alertInfoDStream.filter(_._1)
		//同一设备，每分钟只记录一次预警。  时间单位内 去重
		// 利用es 的id 进行去重   相同mid每分钟的ID是相同的  mid+分钟的时间戳
		val alertInfoWithIdDstream: DStream[(String, AlertInfo)] = filteredAlertInfo.map {
			case (bool, alertInfo) =>
				val minute: Long = alertInfo.ts / 1000 / 60
				//拼接成es的存储的id
				val id: String = alertInfo.mid + "_" + minute
				(id, alertInfo)
		}
		alertInfoWithIdDstream.foreachRDD { rdd =>
			//遍历每个分区的数据
			rdd.foreachPartition {
				alertWithIdItr =>
					val alertWithIdList: List[(String, AlertInfo)] = alertWithIdItr.toList
					 MyEsUtil.insertBulk(GmallConstant.ES_INDEX_ALERT,alertWithIdList)
			}
		}
		ssc.start()
		ssc.awaitTermination()
	}
}
