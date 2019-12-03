package com.atguigu.gmall0624.realtime.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.bean.UserInfo
import com.atguigu.gmall0624.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserApp")
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
		val userInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER, ssc)
		//对数据进行处理操作
		val userInfoDstream: DStream[UserInfo] = userInputDstream.map(_.value()).map(userJson => JSON.parseObject(userJson, classOf[UserInfo]))
		userInfoDstream.print()

		//写入维表
		userInfoDstream.foreachRDD {
			rdd =>
				rdd.foreachPartition {
					userInfoItr => {
						val jedis: Jedis = RedisUtil.getJedisClient
						for (userInfo <- userInfoItr) {
							val userKey: String = "user_info:" + userInfo.id
							val userJson: String = JSON.toJSONString(userInfo, new SerializeConfig(true))
							jedis.set(userKey, userJson)
						}
						jedis.close()
					}
				}
		}
		ssc.start()
		ssc.awaitTermination()
	}
}
