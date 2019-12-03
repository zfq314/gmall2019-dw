package com.atguigu.gmall0624.dw.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * SaleResult
 *
 * @author zhaofuqiang
 * @date 2019/12/3 20:20
 **/
@AllArgsConstructor
@Data
public class SaleResult {
	Long total;
	List<Stat> stats;
	List<Map> detail;
}
