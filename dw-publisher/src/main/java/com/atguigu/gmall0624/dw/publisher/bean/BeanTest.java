package com.atguigu.gmall0624.dw.publisher.bean;

/**
 * BeanTest
 *
 * @author zhaofuqiang
 * @date 2019/12/3 11:29
 **/
public class BeanTest {
	public static void main(String[] args) {
		Test test = new Test("赵富强",25);
		System.out.println(test.toString());
		System.out.println(test.getName());
		System.out.println(test.getAge());
	}
}
