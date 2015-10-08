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
package com.cyngn.vertx.opentsdb.client;

import com.cyngn.vertx.opentsdb.Util;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;

/**
 * Handles publishing metrics to the OpenTsDb consuming verticle
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/7/15
 */
public class MetricPublisher {

    private Logger logger = LoggerFactory.getLogger(MetricPublisher.class);
    private final EventBus bus;
    private final String address;

    /**
     * Constructor
     *
     * @param bus the event bus object to use
     * @param address the address that the vertx-opentsdb verticle was initialized to listen on
     */
    public MetricPublisher(EventBus bus, String address) {
        this.bus = bus;
        this.address = address;
    }

    /**
     * Publish a metric
     *
     * @param name the metric name
     * @param value the metric value
     */
    public void send(String name, String value) {
        send(name, value, null);
    }

    /**
     * Publish a metric
     *
     * @param name the metric name
     * @param value the metric value
     * @param tags the tags to put on the metric
     */
    public void send(String name, String value, JsonObject tags) {
        send(Util.createMetric(name, value, tags));
    }

    /**
     * Publish a metric
     *
     * @param metric a full metric to send
     */
    public void send(TsMetric metric) {
        bus.send(address, metric.asJson());
    }

    /**
     * Publish a metric
     *
     * @param metric a properly constructed metric to send
     */
    public void send(JsonObject metric) {
        if(!TsMetric.isValid(metric)) { throw new IllegalArgumentException("Invalid metric: " + metric); }
        sendMetrics(metric);
    }

    /**
     * Send a list of metrics.
     *
     * @param metrics the metrics to send
     */
    public void sendTsMetricBatch(List<TsMetric> metrics) {
        if (metrics != null && metrics.size() < 1) { throw new IllegalArgumentException("cannot send empty list of metrics"); }

        JsonObject bulkMetric = Util.createBulkMetric(metrics.get(0).asJson());
        for (int i = 1; i < metrics.size(); i++) { Util.addBulkMetric(bulkMetric, metrics.get(i).asJson()); }

        sendMetrics(bulkMetric);
    }

    /**
     * Send a list of properly formatted metrics.
     *
     * @param metrics the metrics to send
     */
    public void sendMetricBatch(List<JsonObject> metrics) {
        if (metrics != null && metrics.size() < 1) { throw new IllegalArgumentException("cannot send empty list of metrics"); }

        JsonObject startMetric = metrics.get(0);
        if (!TsMetric.isValid(startMetric)) { throw new IllegalArgumentException("Invalid bulk metric: " + startMetric);}
        JsonObject bulkMetric = Util.createBulkMetric(startMetric);

        for (int i = 1; i < metrics.size(); i++) {
            JsonObject metric = metrics.get(i);
            if(!TsMetric.isValid(metric)) { throw new IllegalArgumentException("Invalid bulk metric: " + bulkMetric); }
            Util.addBulkMetric(bulkMetric, metrics.get(i));
        }

        sendMetrics(bulkMetric);
    }

    /**
     * Send a metric via the event bus
     * @param message the metric data to send
     */
    private void sendMetrics(JsonObject message) {
        bus.send(address, message, result -> {
            if (result.failed()) {
                logger.error(String.format("Failed to send message on event bus,  address: %s  metrics: %s", address,
                        message.encode()), result.cause());
            }
        });
    }
}
