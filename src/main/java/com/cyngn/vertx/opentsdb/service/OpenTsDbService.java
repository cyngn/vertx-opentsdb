/*
 * Copyright 2015 Cyanogen Inc.
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
package com.cyngn.vertx.opentsdb.service;

import com.cyngn.vertx.opentsdb.OpenTsDbOptions;
import com.cyngn.vertx.opentsdb.service.client.MetricsSender;
import com.cyngn.vertx.opentsdb.service.client.OpenTsDbClient;
import com.google.common.collect.Queues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Handles consuming metrics over the message bus, translating them into OpenTsDb metrics and queueing them up for send
 *  to the OpenTsDb cluster.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/7/14
 */
public class OpenTsDbService extends AbstractVerticle implements Handler<Message<JsonObject>> {

    public final static String ERROR_MESSAGE_ADDRESS = "vertx-opentsdb-errors";
    private Logger logger = LoggerFactory.getLogger(OpenTsDbClient.class);
    public static final String ADD_COMMAND = "add";
    public static final String ADD_ALL_COMMAND = "add_all";
    public static final String ACTION_FIELD = "action";
    private static int FIVE_MINUTES_MILLI = 1000 * 60 * 5;
    private static final String OK_REPLY = "ok";

    private LinkedBlockingQueue<String> metrics;

    private Map<String, Consumer<Message<JsonObject>>> handlers;
    private List<MetricsSender> workers;
    private MetricsParser metricsParser;
    private EventBus eventBus;
    private long flushTimerId = -1;
    private MetricsProcessor processor;
    private long reportingTimerId = -1;
    private OpenTsDbOptions options;

    private String SINGLETON_GUARD_TOPIC = "startup_guard";
    private String THREAD_KEY = "thread_id";

    @Override
    public void start(final Future<Void> startedResult) {
        boolean hasLock = obtainLock();
        if(!hasLock) {
            logger.warn("It appears someone has already deployed the verticle you only need one instance initialized");
            startedResult.fail("Multiple instances should not be started of the service");
            return;
        }

        JsonObject config = context.config();
        options = new OpenTsDbOptions(config);
        eventBus = vertx.eventBus();
        metricsParser = new MetricsParser(options.getPrefix(), options.getDefaultTags(), this::sendError);

        // create the list of workers
        workers = new ArrayList<>(options.getHosts().size());
        metrics = Queues.newLinkedBlockingQueue();

        initializeWorkers(startedResult);
        createMessageHandlers();

        eventBus.consumer(options.getAddress(), this);
    }

    private boolean obtainLock() {
        synchronized (vertx) {
            LocalMap<String, Long> map = vertx.sharedData().getLocalMap(SINGLETON_GUARD_TOPIC);
            Long result = map.putIfAbsent(THREAD_KEY, Thread.currentThread().getId());

            // if put was null then that means we succeeded in getting the lock, if it isn't then it means either
            //  someone else beat us there or it has already been initialized by this thread
            return result == null;
        }
    }

    private void initializeWorkers(Future<Void> startedResult) {
        final AtomicInteger count = new AtomicInteger();
        processor = new MetricsProcessor(workers, options.getMaxBufferBytes(), vertx.eventBus());
        JsonArray hosts = options.getHosts();
        for (int i = 0; i < hosts.size(); i++) {
            JsonObject jsonHost = hosts.getJsonObject(i);

            // we setup one worker dedicated to each endpoint, the same worker always rights to the same outbound socket
            OpenTsDbClient worker = new OpenTsDbClient(jsonHost.getString("host"), jsonHost.getInteger("port"), vertx,
              success -> {
                if(!success) {
                    String error = String.format("Failed to connect to host: %s", jsonHost.encode());
                    logger.error(error);
                    vertx.close();
                    startedResult.fail(error);
                    return;
                }

                count.incrementAndGet();
                if(count.get() == hosts.size()) {
                    flushTimerId = vertx.setPeriodic(options.getFlushInterval(),
                            timerId -> processor.processMetrics(metrics));
                    logger.info(options);
                    startReporter();
                    startedResult.complete();
                }
            });
            workers.add(worker);
        }
    }

    private void startReporter() {
        reportingTimerId = vertx.setPeriodic(FIVE_MINUTES_MILLI, timerId ->
                workers.forEach(MetricsSender::dumpStats));
    }

    @Override
    public void stop() {
        logger.info("Shutting down vertx-opentsdb...");
        if(flushTimerId != -1) { vertx.cancelTimer(flushTimerId); }
        if(reportingTimerId != -1) { vertx.cancelTimer(reportingTimerId); }
        if(metrics.size() > 0) { processor.processMetrics(metrics); }
        workers.forEach(MetricsSender::close);
    }

    private void createMessageHandlers() {
        handlers = new HashMap<>();
        handlers.put(ADD_COMMAND, this::processMetric);
        handlers.put(ADD_ALL_COMMAND, this::processMetricBatch);
    }

    private void processMetricBatch(Message<JsonObject> message) {
        JsonArray metricsObjects = message.body().getJsonArray(MetricsParser.METRICS_FIELD);

        if(metricsObjects == null) {
            String errMsg = String.format("invalid batch message request no 'metrics' field supplied, msg: %s",
                    message.body());
            logger.warn(errMsg);
            sendError(message, errMsg);
        }

        // roll through and add all the metrics
        for (int i = 0; i < metricsObjects.size(); i++) {
            String metric = metricsParser.createMetricString(message, metricsObjects.getJsonObject(i));
            if (metric != null) {
                if (!addMetric(null, metric)) {
                    reportFullBacklog(message);
                    break;
                }
            } else {
                // something is bad in the batch, the parsers error handler will reply to the message as failed
                return;
            }
        }
        message.reply(OK_REPLY);
    }

    private void processMetric(Message<JsonObject> message) {
        final JsonObject tags = message.body().getJsonObject(MetricsParser.TAGS_FIELD);
        if (hasInvalidTags(tags)) {
            // the metric will be rejected by TSD, so don't even send it
            sendError(message, "You specified too many tags");
            return;
        }

        String metricStr = metricsParser.createMetricString(message, message.body());
        if (metricStr != null) {
            addMetric(message, metricStr);
        }
    }

    private boolean addMetric(Message message, String metric) {
        boolean added = true;
        if(isNotFull()) {
            // put the metric in the work queue
            metrics.add(metric);
            if (message != null) { message.reply(OK_REPLY); }
        } else {
            if(message != null) { reportFullBacklog(message); }
            added = false;
        }
        return added;
    }

    private void reportFullBacklog(Message message) {
        String errMsg = String.format("Backlog is at max defined capacity of %d, discarding metric",
                metrics.size());
        logger.warn(errMsg);
        sendError(message, errMsg);
    }

    private boolean hasInvalidTags(JsonObject tags) {
        return tags != null && options.getDefaultTagCount() + tags.size() > options.getMaxTags();
    }

    private boolean isNotFull() {
        return options.getMaxBacklog() == Integer.MIN_VALUE || metrics.size() < options.getMaxBacklog();
    }

    /**
     * Handles processing metric requests off the event bus
     *
     * @param message the metrics message
     */
    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString(ACTION_FIELD);

        if (action == null) { sendError(message, "You must specify an action"); }

        Consumer<Message<JsonObject>> handler = handlers.get(action);

        if (handler != null) { handler.accept(message); }
        else { sendError(message, "Invalid action: " + action + " specified."); }
    }

    private void sendError(Message message, String error) { message.fail(-1, error); }
}
