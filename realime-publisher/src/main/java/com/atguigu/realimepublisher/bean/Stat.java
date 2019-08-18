package com.atguigu.realimepublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 饼图
 * @Date 2019/8/18
 * @Version JDK 1.8
 **/
@Data
@AllArgsConstructor
public class Stat {

    String title;  //饼图的名字

    List<Option> options;
}

