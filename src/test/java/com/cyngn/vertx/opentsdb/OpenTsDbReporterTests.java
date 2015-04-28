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

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.impl.Deployment;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example Java integration test (You need to have OpenTsDb running)
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
//@Ignore("Integration tests, comment out annotation to run the tests")
@RunWith(VertxUnitRunner.class)
public class OpenTsDbReporterTests {

    Vertx vertx;
    private EventBus eb;
    private static String topic = "test-opentsdb";

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
        eb = vertx.eventBus();

        JsonObject config = new JsonObject();
        config.put("address", topic);
        JsonArray array = new JsonArray();
        array.add(new JsonObject().put("host", "localhost").put("port", 4242));
        config.put("hosts", array);
        config.put("maxTags", 1);

        Async async = context.async();
        vertx.deployVerticle(OpenTsDbReporter.class.getName(), new DeploymentOptions().setConfig(config),
                new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> result) {
                if (!result.succeeded()) {
                    result.cause().printStackTrace();
                    context.fail(result.cause());
                }
                async.complete();
            }
        });
    }

    @After
    public void after(TestContext context) {
        Deployment deployment = ((VertxInternal) vertx).getDeployment(vertx.deploymentIDs().iterator().next());
        vertx.undeploy(deployment.deploymentID());
    }


    @Test
    public void testInvalidAction(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();

        Async async = context.async();
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "You must specify an action");
                async.complete();
            }
        });

        Async async1 = context.async();
        metric = new JsonObject().put("action", "badCommand");
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "Invalid action: badCommand specified.");
                async1.complete();
            }
        });
    }

    @Test
    public void testNoTags(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "34.4");
        Async async = context.async();
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "You must specify at least one tag");
                async.complete();
            }
        });
    }

    @Test
    public void testSend(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "34.4");
        metric.put("tags", new JsonObject().put("foo", "bar"));

        Async async = context.async();
        eb.send(topic, metric, new DeliveryOptions(), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                if (result.failed()) {
                    context.fail();
                }

                context.assertEquals("Ok", result.result().body());
                async.complete();
            }
        });
    }

    @Test
    public void testSendMany(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "34.4");
        metric.put("tags", new JsonObject().put("foo", "bar"));

        int totalMessages = 10000;
        AtomicInteger count = new AtomicInteger(0);
        Async async = context.async();

        Handler<AsyncResult<Message<JsonObject>>> handler = result -> {
            if (result.failed()) { context.fail(); }
            context.assertEquals("Ok", result.result().body());
            if(count.incrementAndGet() == totalMessages) { async.complete(); }
        };

        for(int i = 0; i < totalMessages; i++) { eb.send(topic, metric, new DeliveryOptions(), handler); }
    }

    @Test
    public void testTooManyTags(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "test.value");
        metric.put("value", "34.4");
        metric.put("tags",
                new JsonObject().put("foo", "bar")
                        .put("var", "val"));
        Async async = context.async();
        eb.send(topic, metric, new DeliveryOptions(), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "You specified too many tags");
                async.complete();
            }
        });
    }

    @Test
    public void testSendIllegalCharacters(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", OpenTsDbReporter.ADD_COMMAND);
        metric.put("name", "@@@@test@value");
        metric.put("value", "34.4");
        metric.put("tags", new JsonObject().put("foo", "bar"));

        Async async = context.async();
        eb.consumer(OpenTsDbReporter.ERROR_MESSAGE_ADDRESS, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                context.assertEquals(EventBusMessage.INVALID_DATA.toString(), event.body().getString("error"));
                async.complete();
            }
        });

        eb.send(topic, metric, new DeliveryOptions());
    }

}
