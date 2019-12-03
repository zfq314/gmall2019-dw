package com.atguigu.gmall0624.mocker.util;

/**
 * RanOpt
 *
 * @author zhaofuqiang
 * @date 2019/11/25 11:06
 **/
public class RanOpt<T> {
	T value ;
	int weight;

	public RanOpt ( T value, int weight ){
		this.value=value ;
		this.weight=weight;
	}

	public T getValue() {
		return value;
	}

	public int getWeight() {
		return weight;
	}

}
