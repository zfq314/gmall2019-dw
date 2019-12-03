package com.atguigu.gmall0624.realtime.utils


import java.util
import java.util.Properties

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


object MyEsUtil {
	private val properties: Properties = PropertiesUtil.load("config.properties")
	private val ES_HOST: String = properties.getProperty("ES_HOST")
	private val ES_HTTP_PORT: String = properties.getProperty("ES_HTTP_PORT")
	private var factory: JestClientFactory = null

	/**
	 * 获取JestClient的客户端
	 *
	 * @return
	 */
	private def getClient: JestClient = {
		if (factory == null) {
			buider()
		}
		factory.getObject

	}

	/**
	 * 构建工厂类
	 */
	private def buider(): Unit = {
		factory = new JestClientFactory
		factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
			.maxTotalConnection(20) //连接数
			.connTimeout(10000).readTimeout(10000).build)
	}

	/**
	 * 关闭连接
	 *
	 * @param client
	 */
	private def close(client: JestClient): Unit = {
		if (client != null) {
			try
				client.shutdownClient()
			catch {
				case e: Exception =>
					e.printStackTrace()
			}
		}
	}


	def main(args: Array[String]): Unit = {
		//单条数据的插入
		/*val jest: JestClient = getClient

		//创建索引
		val index: Index = new Index.Builder(new Stu(123, "赵富强", 624)).index("stu0624").`type`("stu").id("1").build()
		jest.execute(index)
		close(jest)*/
		//插入多条数据
		val stuList = List(("16", Stu(12, "赵冬梅", 630)), ("66", Stu(15, "赵银苹11111", 631)), ("46", Stu(126, "赵富", 632)))
		insertBulk("stu0626",stuList)


	}

	//插入多个数据
	  def insertBulk(indexName:String,list: List[(String, Any)]): Unit = {

		val jest: JestClient = getClient
		//获取批量插入的对象
		val builderBulk = new Bulk.Builder()
		for ((id, doc) <- list) {
			//里面都是单个的插入值
			val index: Index = new Index.Builder(doc).index(indexName).`type`("_doc").id(id).build()
			//插入所进行的操作
			builderBulk.addAction(index)
		}
		val items: util.List[BulkResult#BulkResultItem] = jest.execute(builderBulk.build()).getItems
		println("保存"+items.size()+"条数据到ES中")
		close(jest)
	}

	//定义一个样例类
	case class Stu(id: Int, name: String, classId: Int)

}
