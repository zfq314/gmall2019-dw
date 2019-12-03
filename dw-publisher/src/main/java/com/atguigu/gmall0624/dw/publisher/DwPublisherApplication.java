package com.atguigu.gmall0624.dw.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.atguigu.gmall0624.dw.publisher.mapper")
public class DwPublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(DwPublisherApplication.class, args);
	}

}
