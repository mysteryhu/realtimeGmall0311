package com.atguigu.realimepublisher.service.serviceImpl;

import com.atguigu.realimepublisher.bean.OrderHourAmount;
import com.atguigu.realimepublisher.mapper.DauMapper;
import com.atguigu.realimepublisher.mapper.OrderMapper;
import com.atguigu.realimepublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.rmi.MarshalledObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Date 2019/8/14
 * @Version JDK 1.8
 **/
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;
    //指定日期活跃用户数量

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //指定日期各时段活跃用户
    @Override
    public Map<String, Long> getDauHours(String date) {

        HashMap dauHourMap = new HashMap<>();
        //查询的结果 为 loghour count(*)
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LOGHOUR"), map.get("CT"));
        }
        return dauHourMap;
    }

    //查询当天订单金额
    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    //查询分时订单金额
    @Override
    public Map<String, Double> getOrderHourAmount(String date) {
        //把List集合转成Map
        HashMap<String, Double> orderHourMap = new HashMap<>();
        List<OrderHourAmount> orderHourAmountList = orderMapper.getOrderHourAmount(date);
        for (OrderHourAmount orderHourAmount : orderHourAmountList) {
            orderHourMap.put(orderHourAmount.getCreateHour(), orderHourAmount.getSumOrderAmount());
        }
        return orderHourMap;
    }

    //读取es数据
    @Override
    public Map<String, Object> getSaleDetailFromES(String date, String keyword, int pageNo, int pageSize) {

        String query = "GET gmall0311_sale_detail/_search\n" +
                "{\n" +
                "  \"query\":{\n" +
                "    \n" +
                "    \"bool\":{\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2019-08-18\"\n" +
                "        }\n" +
                "      }\n" +
                "      ,\n" +
                "      \"must\": {\n" +
                "        \"match\":{\n" +
                "          \"sku_name\":{\n" +
                "            \"query\":\"小米 路由器\",\n" +
                "            \"operator\":\"and\"\n" +
                "          }\n" +
                "        }\n" +
                "      \n" +
                "      }\n" +
                "      \n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\":{  \n" +
                "    \"groupby_gender\":{\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_gender\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 3,\n" +
                "  \"size\": 3\n" +
                "}";
        //官方给定的查询语句api
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构造过滤/匹配条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        //行号 = ( 页面 -1 ) * 每页的行数
        //分页
        searchSourceBuilder.from((pageNo - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        //Search search = new Search.Builder(query).build();
        //查询语句对象
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        //map封装查询结果
        Map<String, Object> resultMap = new HashMap<>();

        try {
            //查询结果
            SearchResult searchResult = jestClient.execute(search);

            resultMap.put("total", searchResult.getTotal());//总数
            //查询的结果封装到map结构体
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

            List<Map> saleList = new ArrayList<>();
            //遍历明细列表,每个hit相当于sql中一行数据
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleList.add(hit.source);
            }
            resultMap.put("saleList", saleList);//明细数据
            //性别聚合结果
            HashMap genderMap = new HashMap<>();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().
                    getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                genderMap.put(bucket.getKey(), bucket.getCount());
            }
            resultMap.put("genderMap", genderMap);//性别聚合

            HashMap ageMap = new HashMap<>();
            List<TermsAggregation.Entry> agebuckets = searchResult.getAggregations().
                    getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry bucket : agebuckets) {
                ageMap.put(bucket.getKey(), bucket.getCount());
            }
            resultMap.put("ageMap", ageMap);//年龄聚合


        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

}
