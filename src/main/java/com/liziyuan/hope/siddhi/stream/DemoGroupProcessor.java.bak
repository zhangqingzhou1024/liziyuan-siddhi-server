package com.liziyuan.hope.siddhi.stream;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zqz
 * @version 1.0
 * @date 2022-02-11 18:51
 */
@Slf4j
public class DemoGroupProcessor {
    public static void main(String[] args) throws InterruptedException {
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "define stream cseEventStream (symbol string, price float, volume long); " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(0) " +
                "select symbol, price, avg(price) as ap, sum(price) as sp, count(price) as cp " +
                "group by symbol " +
                "output first every 4000 milliseconds " +
                "insert into outputStream;";

// Generating runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
// Starting event processing
        siddhiAppRuntime.start();
// Adding callback to retrieve output events from query
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                log.info("============query callback============");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
        /*log.info(timeStamp);
        log.info(inEvents);*/
            }
        });
        siddhiAppRuntime.addCallback("cseEventStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                log.info("============input stream callback============");
                EventPrinter.print(events);
            }
        });


// Retrieving InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");


        int i = 1;
        while (i <= 10) {
            float p = i * 10;
            inputHandler.send(new Object[]{"WSO2", p, 100});
            log.info("\"WSO2\", " + p);
            inputHandler.send(new Object[]{"IBM", p, 100});
            log.info("\"IBM\", " + p);
            Thread.sleep(1000);
            i++;
        }

// Shutting down the runtime
        siddhiAppRuntime.shutdown();

// Shutting down Siddhi
        siddhiManager.shutdown();
    }
}
