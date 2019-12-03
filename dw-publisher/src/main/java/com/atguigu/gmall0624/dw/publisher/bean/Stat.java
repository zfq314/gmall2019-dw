package com.atguigu.gmall0624.dw.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Stat
 *
 * @author zhaofuqiang
 * @date 2019/12/3 20:14
 **/
@Data
@AllArgsConstructor
public class Stat {

	String title;

	List<Option> options;

}
