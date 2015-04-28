/*
 * Copyright 2014 Cyanogen Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cyngn.vertx.opentsdb;

import com.cyngn.vertx.opentsdb.client.MetricsSender;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class MetricsProcessorTests {

    private MetricsSender sender;
    private MetricsSender sender2;
    private AtomicInteger count;
    private AtomicInteger count2;
    private MetricsProcessor processor;
    private AtomicInteger count3;
    private MetricsSender sender3;

    @Before
    public void setUp(){
        count = new AtomicInteger(0);
        sender = new MetricsSender() {
            @Override
            public boolean write(Buffer data) { count.incrementAndGet(); return true; }
            @Override
            public boolean isConnected() { return true; }
            @Override
            public void close() {  }
            @Override
            public void dumpStats() {  }
        };

        count2 = new AtomicInteger(0);
        sender2 = new MetricsSender() {
            @Override
            public boolean write(Buffer data) { count2.incrementAndGet(); return true; }
            @Override
            public boolean isConnected() { return true; }
            @Override
            public void close() {  }
            @Override
            public void dumpStats() {  }
        };

        count3 = new AtomicInteger(0);
        sender3 = new MetricsSender() {
            @Override
            public boolean write(Buffer data) { count3.incrementAndGet(); return true; }
            @Override
            public boolean isConnected() { return true; }
            @Override
            public void close() {  }
            @Override
            public void dumpStats() {  }
        };
    }

    @Test
    public void testProcessing() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        // add more data into queue
        data.add(testStr);
        data.add(testStr);

        processor = new MetricsProcessor(Arrays.asList(sender), testStr.getBytes().length * 3, null);

        processor.processMetrics(data);
        assertEquals(count.intValue(), 1);
    }



    @Test
    public void testMaxBuffer() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        data.add(testStr);
        data.add(testStr);

        processor = new MetricsProcessor(Arrays.asList(sender), testStr.getBytes().length, null);
        processor.processMetrics(data);

        assertEquals(count.intValue(), 2);
    }

    @Test
    public void testMultipleWorkers() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        data.add(testStr);
        data.add(testStr);
        data.add(testStr);
        data.add(testStr);

        processor = new MetricsProcessor(Arrays.asList(sender, sender2, sender3), (testStr.getBytes().length * 2) + 1, null);
        processor.processMetrics(data);

        assertEquals(count.intValue(), 1);
        assertEquals(count2.intValue(), 1);
        assertEquals(count3.intValue(), 1);
    }
}
