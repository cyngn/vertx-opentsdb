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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/11/14
 */
public class MetricsParserTests  {

    private BiConsumer<Message<JsonObject>, String> errorHandler;
    private Integer count;

    private Message<JsonObject> getTestMessage(JsonObject obj) {
        return new Message<JsonObject>() {
            @Override
            public String address() { return null; }

            @Override public MultiMap headers() { return null; }

            @Override public JsonObject body() { return obj; }

            @Override
            public String replyAddress() { return null; }

            @Override
            public void reply(Object message) {  }

            @Override
            public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler) {  }

            @Override
            public void reply(Object message, DeliveryOptions options) {  }

            @Override
            public <R> void reply(Object message, DeliveryOptions options, Handler<AsyncResult<Message<R>>> replyHandler) {  }

            @Override
            public void fail(int failureCode, String message) {  }
        };
    }


    @Before
    public void setUp() {
        count = 0;
        errorHandler = (jsonObjectMessage, s) -> count++;
    }

    @Test
    public void missingNameTest() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("value", "34.4");
        metric.put("tags", new JsonObject().put("foo", "bar"));


        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser(null, "", errorHandler);
        String result = parser.createMetricString(msg);

        assertEquals(null, result);
        assertTrue(count == 1);
    }

    @Test
    public void missingValueTest() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("tags", new JsonObject().put("foo", "bar"));

        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser(null, "", errorHandler);
        String result = parser.createMetricString(msg);

        assertEquals(null, result);
        assertTrue(count == 1);
    }

    @Test
    public void missingTagsTest() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "17");

        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser(null, "", errorHandler);
        String result = parser.createMetricString(msg);

        assertEquals(null, result);
        assertTrue(count == 1);
    }

    @Test
    public void defaultTagsTest() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "17");

        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser(null, "foo=bar", errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.value \\d* 17 foo=bar\\n").matcher(result).matches());
        assertTrue(count == 0);
    }

    @Test
    public void parseTestSimple() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "17");
        metric.put("tags", new JsonObject().put("tag1", "val1").put("tag2", "val2"));

        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser(null, "foo=bar", errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.value \\d* 17 foo=bar tag1=val1 tag2=val2\\n").matcher(result).matches());
        assertTrue(count == 0);
    }

    @Test
    public void parseTestNoDefaultTags() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "17");
        metric.put("tags", new JsonObject().put("tag1", "val1").put("tag2", "val2"));

        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser(null, null, errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.value \\d* 17 tag1=val1 tag2=val2\\n").matcher(result).matches());
        assertTrue(count == 0);
    }

    @Test
    public void parseTestPrefix() {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "17");
        metric.put("tags", new JsonObject().put("tag1", "val1").put("tag2", "val2"));

        Message<JsonObject> msg = getTestMessage(metric);

        MetricsParser parser = new MetricsParser("test.service", "foo=bar", errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.service.test.value \\d* 17 foo=bar tag1=val1 tag2=val2\\n").matcher(result).matches());
        assertTrue(count == 0);
    }
}
