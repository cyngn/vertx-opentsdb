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
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.TCPMetrics;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks TCP/IP connections and data
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/8/15
 */
public class TcpMetricsImpl extends ScheduledMetrics implements TCPMetrics<SocketMetric> {

    private final AtomicLong bytesWritten = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong connectionCount = new AtomicLong();

    private final String BYTES_READ;
    private final String BYTES_WRITTEN;
    private final String CONNECTION_COUNT;

    public TcpMetricsImpl(OpenTsDbOptions options, Vertx vertx, boolean isServer) {
        super(options, vertx);

        String prefix = isServer ? "server." : "client.";
        BYTES_READ = prefix + "bytes_read";
        BYTES_WRITTEN = prefix + "bytes_read";
        CONNECTION_COUNT = prefix + "connection_count";
    }

    @Override
    public TcpMetricsImpl schedule() { return (TcpMetricsImpl) super.schedule(); }

    @Override
    public void bytesRead(SocketMetric socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
        bytesRead.addAndGet(numberOfBytes);
    }

    @Override
    public void bytesWritten(SocketMetric socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
        bytesWritten.addAndGet(numberOfBytes);
    }

    @Override
    public void exceptionOccurred(SocketMetric socketMetric, SocketAddress remoteAddress, Throwable t) {}

    @Override
    void collectMetrics(List<JsonObject> metrics) {
        Util.addMetricIfNonZero(metrics, BYTES_READ, bytesRead.getAndSet(0));
        Util.addMetricIfNonZero(metrics, BYTES_WRITTEN, bytesWritten.getAndSet(0));
        Util.addMetricIfNonZero(metrics, CONNECTION_COUNT, connectionCount.getAndSet(0));
    }

    @Override
    public SocketMetric connected(SocketAddress remoteAddress) {
        connectionCount.incrementAndGet();
        return null;
    }

    @Override
    public void disconnected(SocketMetric socketMetric, SocketAddress remoteAddress) {
        connectionCount.decrementAndGet();
    }
}
