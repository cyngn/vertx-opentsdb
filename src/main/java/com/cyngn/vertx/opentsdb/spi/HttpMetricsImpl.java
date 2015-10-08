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
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.TCPMetrics;

import java.util.List;

/**
 * Generic parent class for tracking HttpMetrics.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/8/15
 */
public class HttpMetricsImpl extends TcpMetricsImpl implements TCPMetrics<SocketMetric> {

    public HttpMetricsImpl(OpenTsDbOptions options, Vertx vertx, boolean isServer) {
        super(options, vertx, isServer);
    }

    @Override
    protected void collectMetrics(List<JsonObject> metrics) {
        super.collectMetrics(metrics);
    }

    @Override
    public SocketMetric connected(SocketAddress remoteAddress) {
        super.connected(remoteAddress);
        return new SocketMetric();
    }

    @Override
    public void disconnected(SocketMetric socketMetric, SocketAddress remoteAddress) {
        super.disconnected(socketMetric, remoteAddress);
    }

    @Override
    public void bytesRead(SocketMetric socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
        super.bytesRead(socketMetric, remoteAddress, numberOfBytes);
        socketMetric.bytesRead += numberOfBytes;
    }

    @Override
    public void bytesWritten(SocketMetric socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
        super.bytesWritten(socketMetric, remoteAddress, numberOfBytes);
        socketMetric.bytesWritten += numberOfBytes;
    }
}
