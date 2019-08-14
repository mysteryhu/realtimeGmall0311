package com.atguigu.realimepublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.realimepublisher.mapper")
public class RealimePublisherApplication {

    public static void main(String[] args) {

        SpringApplication.run(RealimePublisherApplication.class, args);
    }

}
