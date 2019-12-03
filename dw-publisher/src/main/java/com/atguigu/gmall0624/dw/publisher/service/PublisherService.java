package com.atguigu.gmall0624.dw.publisher.service;

import java.util.Map;

public interface PublisherService {
	public long getDauTotal(String date);

	public Map getDauHour(String date);


	public Double getOrderAmount(String date);

	public Map getOrderAmountHour(String date);

	public Map getSaleDetailMap(String dt,String keyword ,int pageNo,int pageSize);
}
