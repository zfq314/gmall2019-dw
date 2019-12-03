package com.atguigu.gmall0624.dw.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
	//求日活的明细
	public long getDauTotal(String date);

	//求日活的分时数
	public List<Map> getDauHour(String date);

}
