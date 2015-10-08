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
package com.cyngn.vertx.opentsdb.spi;

import com.cyngn.vertx.opentsdb.client.MetricPublisher;
import com.cyngn.vertx.opentsdb.OpenTsDbOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.metrics.Metrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles scheduling the collecting and reporting of SPI metrics via the event bus.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/5/15
 */
public abstract class ScheduledMetrics implements Metrics {

    protected final OpenTsDbOptions options;
    private final Vertx vertx;
    private volatile boolean closed;
    protected final MetricPublisher publisher;
    private Logger logger = LoggerFactory.getLogger(ScheduledMetrics.class);

    public ScheduledMetrics(OpenTsDbOptions options, Vertx vertx) {
        this(options, vertx, vertx.eventBus());
    }

    public ScheduledMetrics(OpenTsDbOptions options, Vertx vertx, EventBus bus) {
        publisher = new MetricPublisher(bus, options.getAddress());
        this.options = options;
        this.vertx = vertx;
    }

    /**
     * Primary hook for specializations of this class to report their metrics.
     *
     * @param metrics the metrics to report to OpenTsdb
     */
    abstract void collectMetrics(List<JsonObject> metrics);

    /**
     * Schedule metrics to be reported.
     *
     * @return a reference to 'this' scheduler
     */
    public ScheduledMetrics schedule() {
        vertx.setTimer(options.getSpiPublishInterval(), id -> {
            if(!closed) {
                try {
                    List<JsonObject> metrics = new ArrayList<>();
                    collectMetrics(metrics);
                    if (metrics.size() > 0) {
                        publisher.sendMetricBatch(metrics);
                    }
                } catch (Exception ex) {
                    logger.error(String.format("Failed to publish metrics for %s", this.getClass().getCanonicalName()), ex);
                } finally {
                    // make sure we always reschedule the reporting
                    schedule();
                }
            }
        });

        return this;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void close() {
        closed = true;
    }
}
