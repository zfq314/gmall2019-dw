package com.atguigu.gmall0624.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0624.dw.publisher.bean.Option;
import com.atguigu.gmall0624.dw.publisher.bean.SaleResult;
import com.atguigu.gmall0624.dw.publisher.bean.Stat;
import com.atguigu.gmall0624.dw.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * PublisherController
 *
 * @author zhaofuqiang
 * @date 2019/11/27 13:44
 **/
@RestController
public class PublisherController {
	@Autowired
	PublisherService publisherService;

	/**
	 * 总值
	 *
	 * @param date
	 * @return
	 */
	@GetMapping("realtime-total")
	public String getRealTimeTotal(@RequestParam("date") String date) {
		long dauTotal = publisherService.getDauTotal(date);
		List<Map> totalList = new ArrayList<>();
		Map dauMap = new HashMap();
		dauMap.put("id", "dau");
		dauMap.put("name", "新增日活");
		dauMap.put("value", dauTotal);

		Map newMidMap = new HashMap();
		newMidMap.put("id", "new_mid");
		newMidMap.put("name", "新增设备");
		newMidMap.put("value", 250);

		Map orderAmountMap = new HashMap();
		Double orderAmount = publisherService.getOrderAmount(date);
		orderAmountMap.put("id", "order_amount");
		orderAmountMap.put("name", "新增交易额");
		orderAmountMap.put("value", orderAmount);

		totalList.add(dauMap);
		totalList.add(newMidMap);
		totalList.add(orderAmountMap);
		return JSON.toJSONString(totalList);

	}

	@GetMapping("realtime-hour")
	public String getRealTimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
		if (id.equals("dau")) {
			//根据id不同，取不同的的分时图
			Map dauHourMapTD = publisherService.getDauHour(date);
			String yd = getYD(date);
			Map dauHourMapYD = publisherService.getDauHour(yd);
			Map hourMap = new HashMap();
			hourMap.put("today", dauHourMapTD);
			hourMap.put("yesterday", dauHourMapYD);
			return JSON.toJSONString(hourMap);
		} else if (id.equals("order_amount")) {
			Map orderAmountHourTD = publisherService.getOrderAmountHour(date);//当天
			String yd = getYD(date);
			Map orderAmountHourYD = publisherService.getOrderAmountHour(yd);

			Map hourMap = new HashMap();
			hourMap.put("today", orderAmountHourTD);
			hourMap.put("yesterday", orderAmountHourYD);
			return JSON.toJSONString(hourMap);

		} else {
			return null;
		}

	}

	private String getYD(String td) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date today = simpleDateFormat.parse(td);
			Date yesterday = DateUtils.addDays(today, -1);
			simpleDateFormat.format(yesterday);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	@GetMapping("sale_detail")
	public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int startpage, @RequestParam("size") int size, @RequestParam("keyword") String keyword) {
		//调用业务
		Map resultMap = publisherService.getSaleDetailMap(date, keyword, startpage, size);
		//获取总数

		Long total = (Long) resultMap.get("total");
		List<Map> detailList = (List<Map>) resultMap.get("detail");
		Map genderAgg = (Map) resultMap.get("genderAgg");
		Map ageAgg = (Map) resultMap.get("ageAgg");
		//System.out.println(total+"--"+detailList+"----"+genderAgg+"------"+ageAgg);
		//处理数据
		Long age20ct = 0L;
		Long age20ct_30ct = 0L;
		Long age30ct = 0L;

		for (Object o : ageAgg.entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			String ageStr = (String) entry.getKey();
			Long count = (Long) entry.getValue();
			Integer age = Integer.parseInt(ageStr);

			if (age >= 30) {
				age30ct += count;
			} else if (age >= 20 && age < 30) {
				age20ct_30ct += count;
			} else {
				age20ct += count;
			}
		}
		Double age_20rt = Math.round(age20ct * 1000D / total) / 10D;
		Double age_20_30rt = Math.round(age20ct_30ct * 1000D / total) / 10D;
		Double age_30rt = Math.round(age30ct * 1000D / total) / 10D;


		List<Option> ageOptionList = new ArrayList<>();
		ageOptionList.add(new Option("20岁以下", age_20rt));
		ageOptionList.add(new Option("20岁-30岁", age_20_30rt));
		ageOptionList.add(new Option("30岁以上", age_30rt));

		Stat ageStat = new Stat("年龄占比", ageOptionList);
		Long maleCt = genderAgg.get("M") != null ? (Long) genderAgg.get("M") : 0;
		Long femaleCt = genderAgg.get("F") != null ? (Long) genderAgg.get("F") : 0;

		Double maleRt = Math.round(maleCt * 1000D / total) / 10D;
		Double femalRt = Math.round(femaleCt * 1000D / total) / 10D;

		List<Option> genderOptionList = new ArrayList<>();
		genderOptionList.add(new Option("男", maleRt));
		genderOptionList.add(new Option("女", femalRt));
		Stat genderStat = new Stat("性别占比", genderOptionList);
		List<Stat> statList=new ArrayList<>();
		statList.add(ageStat);
		statList.add(genderStat);
		SaleResult saleResult = new SaleResult(total, statList, detailList);

		return JSON.toJSONString(saleResult);
	}
}
