package com.atguigu.gmall0624.dw.publisher.service.impl;

import com.atguigu.gmall0624.common.constant.GmallConstant;
import com.atguigu.gmall0624.dw.publisher.mapper.DauMapper;
import com.atguigu.gmall0624.dw.publisher.mapper.OrderMapper;
import com.atguigu.gmall0624.dw.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PublisherServiceImpl
 *
 * @author zhaofuqiang
 * @date 2019/11/27 13:45
 **/
@Service
public class PublisherServiceImpl implements PublisherService {
	@Autowired
	DauMapper dauMapper;
	@Autowired
	OrderMapper orderMapper;
	@Autowired
	JestClient jestClient;

	@Override
	public long getDauTotal(String date) {
		long dauTotal = dauMapper.getDauTotal(date);
		return dauTotal;
	}

	@Override
	public Map getDauHour(String date) {
		List<Map> dauHourMapList = dauMapper.getDauHour(date);
		Map dauHourMap = new HashMap<>();
		for (Map map : dauHourMapList) {
			dauHourMap.put(map.get("loghour"), map.get("ct"));
		}
		return dauHourMap;

	}

	@Override
	public Double getOrderAmount(String date) {
		return orderMapper.selectOrderAmount(date);
	}

	@Override
	public Map getOrderAmountHour(String date) {
		List<Map> mapList = orderMapper.selectOrderAmountHour(date);
		Map hourAmountMap = new HashMap();
		for (Map colVaMap : mapList) {
			hourAmountMap.put(colVaMap.get("CREATE_HOUR"), colVaMap.get("AMOUNT"));
		}
		return hourAmountMap;
	}

	@Override
	public Map getSaleDetailMap(String dt, String keyword, int pageNo, int pageSize) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//查询
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.filter(new TermQueryBuilder("dt", dt));
		boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
		searchSourceBuilder.query(boolQueryBuilder);

		//聚合
		//用工具类
		TermsBuilder genderAgg = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
		TermsBuilder ageAgg = AggregationBuilders.terms("groupby_age").field("user_age").size(120);
		searchSourceBuilder.aggregation(genderAgg);
		searchSourceBuilder.aggregation(ageAgg);
		//分页
		//分页的计算
		int rowNum = (pageNo - 1) * pageSize;
		searchSourceBuilder.from(rowNum);

		searchSourceBuilder.size(pageSize);
		System.out.println(searchSourceBuilder.toString());
		Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DETAIL).addType("_doc").build();
		Map resultMap = new HashMap();
		try {
			SearchResult searchResult = jestClient.execute(search);
			Long total = searchResult.getTotal();

			List<Map> detaliList = new ArrayList<>();
			List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
			for (SearchResult.Hit<Map, Void> hit : hits) {
				detaliList.add(hit.source);
			}
			//性别聚合结果
			List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
			Map genderMap = new HashMap();
			for (TermsAggregation.Entry genderBucket : genderBuckets) {
				genderMap.put(genderBucket.getKey(), genderBucket.getCount());
			}
			//年龄聚合
			List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
			Map ageMap = new HashMap();
			for (TermsAggregation.Entry ageBucket : ageBuckets) {
				ageMap.put(ageBucket.getKey(), ageBucket.getCount());
			}
			resultMap.put("total", total);
			resultMap.put("detail", detaliList);
			resultMap.put("genderAgg", genderMap);
			resultMap.put("ageAgg", ageMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultMap;
	}


}
