package com.atguigu.gmall0311.cannal.client;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @Date 2019/8/14
 * @Version JDK 1.8
 **/
public class CannalClient {

    public static void main(String[] args) {

        //TODO 1 与canalClient建立连接

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");

        //TODO 2 canalClient 向 canal服务器抓取数据

        while(true){
            canalConnector.connect();  //尝试连接服务器
            canalConnector.subscribe("gmall0311.*"); //获取数据库下的所有表数据变化信息
            Message message = canalConnector.get(100);//一次抓取100个topic的数据
            //如果没抓取到
            if (message.getEntries().size()==0){
                try {
                    System.out.println("没抓到数据,休息5s");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //一个entry就相当于一条SQL
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //只有行变化的数据才进行处理

                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //反序列化entry中的数据
                            ByteString storeValue = entry.getStoreValue();
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //rowData 相当于表中一行数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType(); //操作(事件)类型 insert delete update
                        String tableName = entry.getHeader().getTableName();//从entry中获取表名

                        CanalHanlder canalHanlder = new CanalHanlder(tableName, eventType, rowDatasList);
                        canalHanlder.handle();
                    }
                }
            }

        }
    }
}
