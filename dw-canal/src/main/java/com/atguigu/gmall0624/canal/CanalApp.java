package com.atguigu.gmall0624.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * CanalApp
 *
 * @author zhaofuqiang
 * @date 2019/11/27 20:24
 **/
public class CanalApp {
	public static void main(String[] args) {

		//建立和canal的连接
		CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
		while (true) {
			canalConnector.connect();
			canalConnector.subscribe("*.*");
			Message message = canalConnector.get(100);

			int size = message.getEntries().size();
			if (size == 0) {
				try {
					System.out.println("没有数据休息5秒");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				for (CanalEntry.Entry entry : message.getEntries()) {
					//只有是数据相关处理的时候才要数据
					if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {

						ByteString storeValue = entry.getStoreValue();
						CanalEntry.RowChange rowChange = null;
						try {
							rowChange = CanalEntry.RowChange.parseFrom(storeValue);

						} catch (InvalidProtocolBufferException e) {
							e.printStackTrace();
						}
						//行集
						List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
						String tableName = entry.getHeader().getTableName();//表名
						CanalEntry.EventType eventType = rowChange.getEventType();//操作类型
						CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
						canalHandler.handle();
					}

				}
			}
		}
		//订阅数据

		//抓取message
	}
}
