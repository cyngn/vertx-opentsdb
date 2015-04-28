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
import com.cyngn.vertx.opentsdb.client.OpenTsDbClient;
import com.google.common.collect.Queues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;

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
public class OpenTsDbReporter extends AbstractVerticle implements Handler<Message<JsonObject>> {

    public final static String ERROR_MESSAGE_ADDRESS = "vertx-opentsdb-errors";
    private Logger logger = LoggerFactory.getLogger(OpenTsDbClient.class);
    public static final String ADD_COMMAND = "add";
    public static final int OPENTSDB_DEFAULT_MAX_TAGS = 8;
    private static int FIVE_MINUTES_MILLI = 1000 * 60 * 5;

    private JsonArray hosts;
    private final int DEFAULT_MTU = 1500;
    private int maxBufferSizeInBytes;
    private LinkedBlockingQueue<String> metrics;
    private int maxTags;
    private int defaultTagCount;

    private Map<String, Consumer<Message<JsonObject>>> handlers;
    private List<MetricsSender> workers;
    private String address;
    private MetricsParser metricsParser;
    private EventBus eventBus;
    private Integer flushInterval;
    private Integer maxBacklog;
    private long flushTimerId = -1;
    private MetricsProcessor processor;
    private String defaultTags;
    private long reportingTimerId = -1;

    @Override
    public void start(final Future<Void> startedResult) {

        JsonObject config = context.config();
        hosts = config.getJsonArray("hosts", new JsonArray("[{ \"host\" : \"localhost\", \"port\" : 4242}]"));
        maxBufferSizeInBytes = config.getInteger("maxBufferSizeInBytes", DEFAULT_MTU);
        String prefix = config.getString("prefix", null);
        address = config.getString("address", "vertx.opentsdb-reporter");
        maxTags = config.getInteger("maxTags", OPENTSDB_DEFAULT_MAX_TAGS);
        flushInterval = config.getInteger("flushIntervalMilli", 1000);
        maxBacklog = config.getInteger("maxMetricBacklog", Integer.MIN_VALUE);

        final JsonObject configuredTags = config.getJsonObject("tags");
        if (configuredTags != null && configuredTags.size() > maxTags) {
            startedResult.fail(new IllegalStateException("Found more default tags than the max (" + maxTags + ")"));
        }
        defaultTagCount = configuredTags != null ? configuredTags.size() : 0;
        defaultTags = Util.createTagsFromJson(configuredTags);

        eventBus = vertx.eventBus();

        metricsParser = new MetricsParser(prefix, defaultTags, this::sendError);

        // create the list of workers
        workers = new ArrayList<>(hosts.size());
        metrics = Queues.newLinkedBlockingQueue();

        initializeWorkers(startedResult);
        createMessageHandlers();

        eventBus.consumer(address, this);
    }

    private void outputConfig() {
        StringBuilder builder = new StringBuilder();
        builder.append("Config[maxBufferSize=").append(maxBufferSizeInBytes).append(", address=").append(address)
                .append(", maxTags=").append(maxTags).append(", flushInterval=").append(flushInterval)
                .append("(ms), maxBacklog=").append(maxBacklog == Integer.MIN_VALUE ? "INFINITY" : maxBacklog);
        if (defaultTagCount > 0) {
            builder.append(", tags='").append(defaultTags).append("'");
        }
        builder.append(", hosts='").append(hosts.encode()).append("']");
    }

    private void initializeWorkers(Future<Void> startedResult) {
        final AtomicInteger count = new AtomicInteger();
        processor = new MetricsProcessor(workers, maxBufferSizeInBytes, vertx.eventBus());
        for (int i = 0; i < hosts.size(); i++) {
            JsonObject jsonHost = hosts.getJsonObject(i);

            // we setup one worker dedicated to each endpoint, the same worker always rights to the same outbound socket
            OpenTsDbClient worker = new OpenTsDbClient(jsonHost.getString("host"), jsonHost.getInteger("port"), vertx,
              success -> {
                if(!success) {
                    logger.error(String.format("Failed to connect to host: %s", jsonHost.encode()));
                    vertx.close();
                }

                count.incrementAndGet();
                if(count.get() == hosts.size()) {
                    flushTimerId = vertx.setPeriodic(flushInterval, timerId -> processor.processMetrics(metrics));
                    outputConfig();
                    startReporter();
                    startedResult.complete();
                }
            });
            workers.add(worker);
        }
    }

    private void startReporter() {
        reportingTimerId = vertx.setPeriodic(FIVE_MINUTES_MILLI, timerId ->
                workers.forEach(com.cyngn.vertx.opentsdb.client.MetricsSender::dumpStats));
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
    }

    private void processMetric(Message<JsonObject> message) {
        final JsonObject tags = message.body().getJsonObject("tags");
        if (tags != null && defaultTagCount + tags.size() > maxTags) {
            // the metric will be rejected by TSD, so don't even send it
            sendError(message, "You specified too many tags");
            return;
        }

        String metricStr = metricsParser.createMetricString(message);
        if (metricStr != null) {
            if(maxBacklog == Integer.MIN_VALUE || metrics.size() < maxBacklog) {
                // put the metric in the work queue
                metrics.add(metricStr);
                message.reply("Ok");
            } else {
                String errMsg = String.format("Backlog is at max defined capacity of %d, discarding metric", metrics.size());
                logger.warn(errMsg);
                sendError(message, errMsg);
            }
        }
    }

    /**
     * Handles processing metric requests off the event bus
     *
     * @param message the metrics message
     */
    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString("action");

        if (action == null ) { sendError(message, "You must specify an action"); }

        Consumer<Message<JsonObject>> handler = handlers.get(action);

        if ( handler != null) { handler.accept(message); }
        else { sendError(message, "Invalid action: " + action + " specified."); }
    }

    private void sendError(Message message, String error) {
        message.fail(-1, error);
    }
}
