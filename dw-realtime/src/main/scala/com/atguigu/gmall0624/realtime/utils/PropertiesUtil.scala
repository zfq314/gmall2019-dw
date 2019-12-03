package com.atguigu.gmall0624.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
	def main(args: Array[String]): Unit = {
		val properties: Properties = PropertiesUtil.load("config.properties")

		println(properties.getProperty("kafka.broker.list"))
		println(properties.getProperty("ES_HOST"))
		println(properties.getProperty("ES_HTTP_PORT"))
	}

	def load(propertieName:String): Properties ={
		val prop=new Properties();
		prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
		prop
	}

}
