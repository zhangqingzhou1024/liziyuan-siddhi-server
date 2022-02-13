package com.liziyuan.hope.siddhi.stream;

import com.alibaba.fastjson.JSON;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import lombok.extern.slf4j.Slf4j;

/**
 * 流式处理 = 数据过滤
 *
 * @author zqz
 * @version 1.0
 * @date 2022-02-12 22:40
 */
@Slf4j
public class StreamFilterProcessor {

    static final int MAX_SOURCE_DATA_SIZE = 10;

    public static void main(String[] args) throws InterruptedException {
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = getSiddhiSql();

        // Generating runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        // Starting event processing
        siddhiAppRuntime.start();

        // filterStreamDataQuery
        siddhiAppRuntime.addCallback("filterStreamDataQuery", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                log.info("============filterStreamDataQuery callback============");
                log.info("filterStreamDataQuery callback= > {}", inEvents);
            }
        });
        // outputStream 过滤完的数据，看业务需求如何处理，比如 写入数据库、调接口等
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    try {
                        log.info("final data is {}", event);
                    } catch (Exception e) {
                        log.info("final data：{}，exception: {}", event.getData(), e.getMessage());
                        break;
                    }
                }
            }
        });

        // 数据生产者 Retrieving InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        for (int i = 0; i < MAX_SOURCE_DATA_SIZE; i++) {
            String sourceData = getSourceData(i);
            String jsonString = JSON.toJSONString(sourceData);
            Object[] data = {sourceData};
            inputHandler.send(data);
            Thread.sleep(500);
        }

        Thread.sleep(20000);
        // Shutting down the runtime & Siddhi
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * 获取 siddhiSql
     *
     * @return siddhiSql
     */
    private static String getSiddhiSql() {
        String siddhiSql = "define stream StockStream  (jsonString string);\n" +
                "@info(name = 'convertToJsonQuery') " +
                "from StockStream select json:toObject(jsonString) as jsonObj insert into InputStream ;\n" +
                "@info(name = 'filterStreamDataQuery') from \n" +
                "InputStream [\n" +
                "(json:getString(jsonObj,'$.source.table_name')  =='domain.student' )\n" +
                "and\n" +
                "(((json:getLong(jsonObj,'$.payload.id')  >= 3 )))\n" +
                "] \n" +
                "select jsonObj insert into outputStream;";
        log.info("siddhiSql=>{}", siddhiSql);
        return siddhiSql;
    }

    private static String getSourceData(long id) {
        String dataJsonStr = "{\n" +
                "  \"payload\": {\n" +
                "    \"id\": " + id + ",\n" +
                "    \"create_time\": null,\n" +
                "    \"update_time\": null,\n" +
                "    \"name\": \"zhangsan\"\n" +
                "  },\n" +
                "  \"key\": \"9eb79285c2d649fe8f2c85f63fa29e14\",\n" +
                "  \"source\": {\n" +
                "    \"connector\": \"postgresql\",\n" +
                "    \"name\": \"jdbc_storage\",\n" +
                "    \"ts_ms\": 1644393616714,\n" +
                "    \"db\": \"jdbc_storage\",\n" +
                "    \"schema\": \"domain\",\n" +
                "    \"table\": \"asd_0209\",\n" +
                "    \"table_name\": \"domain.student\"\n" +
                "  },\n" +
                "  \"op\": \"c\",\n" +
                "  \"ts_ms\": 1644393617031\n" +
                "}";
        log.info("dataJsonStr => {}", id);
        return dataJsonStr;
    }
}
