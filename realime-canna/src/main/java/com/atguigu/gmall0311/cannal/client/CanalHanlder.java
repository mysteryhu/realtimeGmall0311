package com.atguigu.gmall0311.cannal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0311.cannal.util.MyKafkaSender;
import com.atguigu.realimeGmall0311.common.constant.GmallConstants;

import java.util.List;


public class CanalHanlder {

    String tableName; //表名
    CanalEntry.EventType eventType; //事件类型
    List<CanalEntry.RowData> rowDataList; //行级


    public CanalHanlder(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }


    public void handle() {
        if (tableName.equals("order_info")&&eventType.equals(CanalEntry.EventType.INSERT)){
            //遍历List 每一行都发送给kfk
            for (CanalEntry.RowData rowData : rowDataList) {
                sendToKafka(GmallConstants.KAFKA_TOPIC_ORDER,rowData);
            }
        }
    }

    /**
     * 发送给kafka
     * @param topic
     * @param rowData
     */
    private void sendToKafka(String topic, CanalEntry.RowData rowData) {
        //插入操作,取变化后的数据
        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
        //JSON 对象放置每一行的所有列名:列值
        JSONObject jsonObject = new JSONObject();
        //遍历每一个列值
        for (CanalEntry.Column column : afterColumnsList) {
            System.out.println(column.getName() + "------>" + column.getValue());
            jsonObject.put(column.getName(),column.getValue());
        }
        String rowJson = jsonObject.toJSONString();
        //发送到kafka中
        MyKafkaSender.send(topic,rowJson);

    }
}





