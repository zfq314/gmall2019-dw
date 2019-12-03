package com.atguigu.gmall0624.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0624.realtime.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization

object SaleApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
		//获取数据
		val inputOrderDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
		val inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_DETAIL, ssc)
		inputOrderDstream.print()

		//对数据进行转换的操作
//		val orderInfoDstream: DStream[OrderInfo] = inputOrderDstream.map {
//			record =>
//				val jsonString: String = record.value()
//				val ordrInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
//				//对电话进行脱敏的操作
//				val telTuple: (String, String) = ordrInfo.consignee_tel.splitAt(3)
//				val telTail: (String, String) = telTuple._2.splitAt(4)
//				ordrInfo.consignee_tel = telTuple._1 + "*****" + telTail
//
//				//改变日期的结构
//				val dateTimeArray: Array[String] = ordrInfo.create_time.split(" ")
//				ordrInfo.create_date = dateTimeArray(0)
//				ordrInfo.create_hour = dateTimeArray(1).split(":")(0)
//				// 订单上增加一个字段   该订单是否是该用户首次下单
//				//维护一个 状态   该用户是否过单的状态  // redis 可以   // mysql 可以
//				ordrInfo
//		}
//		//orderInfoDstream.print()
//		val orderDitailDstream: DStream[OrderDetail] = inputOrderDetailDstream.map(_.value()).map(JSON.parseObject(_, classOf[OrderDetail]))
//		val orderInfoWithIdDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
//		//
//		val orderDetailWithIdStream: DStream[(String, OrderDetail)] = orderDitailDstream.map(orderDetail => (orderDetail.id, orderDetail))
//		//orderDetailWithIdStream.print()
//		val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoWithIdDstream.join(orderDetailWithIdStream)
//		value.print()
//		//双流join
//		val orderFullJoinedDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithIdStream)
//
//		orderFullJoinedDstream.print()
//		val saleDetailDstream: DStream[SaleDetail] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
//			implicit val formats = org.json4s.DefaultFormats
//			var saleDetailListBuffer = ListBuffer[SaleDetail]()
//			val jedis: Jedis = RedisUtil.getJedisClient
//			if (orderInfoOption != None) {
//				//组合SaleDetail
//				val orderInfo: OrderInfo = orderInfoOption.get
//				if (orderDetailOption != None) {
//					val orderDetail: OrderDetail = orderDetailOption.get
//					val saleDetail = new SaleDetail(orderInfo, orderDetail)
//					saleDetailListBuffer += saleDetail
//				}
//				//2 写缓存
//				// redis key设计   type  string       key   order_info:[order_id]   value  order_info_json
//				var orderInfoKey = "order_info:" + orderInfo.id
//				val orderInfoJson: String = Serialization.write(orderInfo)
//				jedis.setex(orderInfoKey, 600, orderInfoJson)
//				//读缓存
//				val orderDetailKey: String = "order_detail:" + orderInfo.id
//				val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
//				if (orderDetailSet != null && orderDetailSet.size() > 0) {
//					import scala.collection.JavaConversions._
//					for (orderDetailJsonString <- orderDetailSet) {
//						val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonString, classOf[OrderDetail])
//						val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
//						saleDetailListBuffer += saleDetail
//					}
//				}
//			} else {
//				val orderDetail: OrderDetail = orderDetailOption.get
//				val orderDetailKey: String = "order_detail:" + orderDetail.order_id
//				val orderDetailJson: String = Serialization.write(orderDetail)
//				jedis.sadd(orderDetailKey, orderDetailJson)
//				jedis.expire(orderDetailKey, 600)
//
//				//读取缓存
//				val orderInfoKey: String = "order_info:" + orderDetail.order_id
//				if (orderDetailJson != null && orderDetailJson.size > 0) {
//					val orderInfo: OrderInfo = JSON.parseObject(orderDetailJson, classOf[OrderInfo])
//					val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
//					saleDetailListBuffer += saleDetail
//				}
//			}
//			jedis.close()
//			saleDetailListBuffer
//		}
//		saleDetailDstream.print()
//		//关联维表
//		val saleDetailFinalDstream: DStream[SaleDetail] = saleDetailDstream.mapPartitions {
//			saleDetailItr =>
//				val jedis: Jedis = RedisUtil.getJedisClient
//				val saleDetailListBuffer: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
//				for (saleDetail <- saleDetailItr) {
//					val userKey: String = "user_info:" + saleDetail.user_id
//					val userJson: String = jedis.get(userKey)
//					if (userJson != null && userJson.length > 0) {
//						val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
//						saleDetail.mergeUserInfo(userInfo)
//						saleDetailListBuffer += saleDetail
//					}
//				}
//				jedis.close()
//				saleDetailListBuffer.iterator
//		}
//		saleDetailFinalDstream.print()
//		//写入ES
//		saleDetailFinalDstream.foreachRDD {
//			rdd =>
//				rdd.foreachPartition { saleDetailItr =>
//					val saleDetailWithIdList: List[(String, SaleDetail)] = saleDetailItr.map(saleDetail => (saleDetail.order_detail_id, saleDetail)).toList
//					MyEsUtil.insertBulk(GmallConstant.ES_INDEX_DETAIL, saleDetailWithIdList)
//				}
//		}
		ssc.start()
		ssc.awaitTermination()
	}
}
