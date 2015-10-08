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

import com.cyngn.vertx.opentsdb.OpenTsDbOptions;
import com.cyngn.vertx.opentsdb.Util;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.metrics.EventBusMetrics;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Tracks EventBus usage.
 *
 * TODO: Right now message replies make this class really chatty see https://bugs.eclipse.org/bugs/show_bug.cgi?id=480240
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/8/15
 */
public class EventBusMetricsImpl extends ScheduledMetrics implements EventBusMetrics {

    private final AtomicReference<ConcurrentHashMap<String, AtomicLong>> sent = new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicReference<ConcurrentHashMap<String, AtomicLong>> received = new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicReference<ConcurrentHashMap<String, AtomicLong>> bytesSent = new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicReference<ConcurrentHashMap<String, AtomicLong>> bytesReceived = new AtomicReference<>(new ConcurrentHashMap<>());

    private static final String EVENT_BUS = "event_bus.";
    private static final String SENT = ".sent";
    private static final String RECEIVED = ".received";
    private static final String SENT_BYTES = ".sent_bytes";
    private static final String RECEIVED_BYTES = ".received_bytes";

    public EventBusMetricsImpl(OpenTsDbOptions options, Vertx vertx, EventBus bus) {
        super(options, vertx, bus);
    }

    @Override
    public EventBusMetricsImpl schedule() { return (EventBusMetricsImpl) super.schedule(); }

    @Override
    public Object handlerRegistered(String address, boolean replyHandler) { return null; }

    @Override
    public void handlerUnregistered(Object handler) {  }

    @Override
    public void beginHandleMessage(Object handler, boolean local) {  }

    @Override
    public void endHandleMessage(Object handler, Throwable failure) {  }

    @Override
    public void messageSent(String address, boolean publish, boolean local, boolean remote) {
        sent.get().computeIfAbsent(address, key -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public void messageReceived(String address, boolean publish, boolean local, int handlers) {
        received.get().computeIfAbsent(address, key -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public void messageWritten(String address, int numberOfBytes) {
        bytesSent.get().computeIfAbsent(address, key -> new AtomicLong()).addAndGet(numberOfBytes);
    }

    @Override
    public void messageRead(String address, int numberOfBytes) {
        bytesReceived.get().computeIfAbsent(address, key -> new AtomicLong()).addAndGet(numberOfBytes);
    }

    @Override
    public void replyFailure(String address, ReplyFailure failure) {

    }

    @Override
    protected void collectMetrics(List<JsonObject> metrics) {
        ConcurrentHashMap<String, AtomicLong> copiedSent = sent.getAndSet(new ConcurrentHashMap<>());
        ConcurrentHashMap<String, AtomicLong> copiedReceived = received.getAndSet(new ConcurrentHashMap<>());
        ConcurrentHashMap<String, AtomicLong> copiedSentBytes = bytesSent.getAndSet(new ConcurrentHashMap<>());
        ConcurrentHashMap<String, AtomicLong> copiedReceivedBytes = bytesReceived.getAndSet(new ConcurrentHashMap<>());

        convertToMetrics(metrics, copiedSent, SENT);
        convertToMetrics(metrics, copiedReceived, RECEIVED);
        convertToMetrics(metrics, copiedSentBytes, SENT_BYTES);
        convertToMetrics(metrics, copiedReceivedBytes, RECEIVED_BYTES);
    }

    private static void convertToMetrics(List<JsonObject> metrics, ConcurrentHashMap<String, AtomicLong> data,
                                         String suffix) {
        metrics.addAll(data.entrySet().stream().map(entry ->
                Util.createRawMetric(EVENT_BUS + entry.getKey() + suffix,
                        entry.getValue().toString())).collect(Collectors.toList()));
    }
}
