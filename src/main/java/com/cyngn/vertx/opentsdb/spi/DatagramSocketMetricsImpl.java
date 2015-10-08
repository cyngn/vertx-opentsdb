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
import io.vertx.core.spi.metrics.DatagramSocketMetrics;

import java.util.List;

/**
 * Datagram socket tracker, not currently implemented.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/8/15
 */
public class DatagramSocketMetricsImpl extends ScheduledMetrics implements DatagramSocketMetrics {

    public DatagramSocketMetricsImpl(OpenTsDbOptions options, Vertx vertx) {
        super(options, vertx);
    }

    @Override
    public void listening(SocketAddress localAddress) {  }

    @Override
    public void bytesRead(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) {  }

    @Override
    public void bytesWritten(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) {  }

    @Override
    public void exceptionOccurred(Void socketMetric, SocketAddress remoteAddress, Throwable t) {  }

    @Override
    protected void collectMetrics(List<JsonObject> metrics) {}

    @Override
    public boolean isEnabled() { return true; }

    @Override
    public void close() {  }
}
