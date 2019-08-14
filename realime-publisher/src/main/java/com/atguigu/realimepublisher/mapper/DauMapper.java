package com.atguigu.realimepublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

      Long selectDauTotal(String date);
      List<Map> selectDauTotalHourMap(String date);
}
