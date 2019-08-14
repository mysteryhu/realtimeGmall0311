package com.atguigu.realimeGmall0311.mock.Util;

/**
 * @Date 2019/8/12
 * @Version JDK 1.8
 **/
public class RanOpt<T>{
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

