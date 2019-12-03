package com.atguigu.gmall0624.mocker.util;

import java.util.Random;

/**
 * RandomNum
 *
 * @author zhaofuqiang
 * @date 2019/11/25 11:08
 **/
public class RandomNum {
	public static final  int getRandInt(int fromNum,int toNum){
		return   fromNum+ new Random().nextInt(toNum-fromNum+1);
	}

}
