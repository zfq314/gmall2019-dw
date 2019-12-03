package com.atguigu.gmall0624logger.logger.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0624.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;




/**
 * LoggerController
 *
 * @author zhaofuqiang
 * @date 2019/11/25 12:46
 **/
@RestController
@Slf4j
public class LoggerController {
	@Autowired
	KafkaTemplate<String,String> kafkaTemplate;

	@PostMapping
	public  String log( @RequestParam("logString") String logString){
		JSONObject jsonObject = JSONObject.parseObject(logString);
		jsonObject.put("ts",System.currentTimeMillis());
		String jsonString = jsonObject.toJSONString();
		log.info(jsonString);
		if (jsonObject.get("type").equals("startup")){
			kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
		}else {
			kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
		}
		return "ok";
	  }
}
