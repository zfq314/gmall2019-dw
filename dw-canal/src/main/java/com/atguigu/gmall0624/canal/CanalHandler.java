package com.atguigu.gmall0624.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0624.canal.utils.MyKafkaSender;
import com.atguigu.gmall0624.common.constant.GmallConstant;

import java.util.List;

/**
 * CanalHandler
 *
 * @author zhaofuqiang
 * @date 2019/11/27 20:24
 **/
public class CanalHandler {
	CanalEntry.EventType eventType;

	String tableName;

	List<CanalEntry.RowData> rowDataList;

	public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
		this.eventType = eventType;
		this.tableName = tableName;
		this.rowDataList = rowDataList;
	}

	public void handle() {
		if (tableName.equals("order_info") && eventType.equals(CanalEntry.EventType.INSERT)) { //下单
			rowDateToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
		} else if (tableName.equals("order_detail")&&eventType.equals(CanalEntry.EventType.INSERT)) {
			rowDateToKafka(GmallConstant.KAFKA_TOPIC_DETAIL);
		}else if(tableName.equals("user_info")&&(eventType.equals(CanalEntry.EventType.INSERT)||eventType.equals(CanalEntry.EventType.UPDATE) )){
			rowDateToKafka(GmallConstant.KAFKA_TOPIC_USER);
		}


	}


	private void rowDateToKafka(String topic) {
		for (CanalEntry.RowData rowData : rowDataList) {
			List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
			JSONObject jsonObject = new JSONObject();
			for (CanalEntry.Column column : afterColumnsList) {
				jsonObject.put(column.getName(), column.getValue());
				System.out.println(jsonObject.toJSONString());
			}
			MyKafkaSender.send(topic, jsonObject.toJSONString());

		}
	}
}
