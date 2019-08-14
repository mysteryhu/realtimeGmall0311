package com.atguigu.realimeGmall0311.mock.Util;

/**
 * @Date 2019/8/12
 * @Version JDK 1.8
 **/
import java.util.Random;

public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}
