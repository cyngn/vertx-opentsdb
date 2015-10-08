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
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.*;

/**
 * OpenTsDb Impl of the Vert.x Metrics SPI interface.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 9/22/15
 */
public class VertxMetricsImpl implements VertxMetrics {

    private final Vertx vertx;
    private final OpenTsDbOptions options;
    private Logger logger = LoggerFactory.getLogger(VertxMetricsImpl.class);

    public VertxMetricsImpl(OpenTsDbOptions options, Vertx vertx) {
        this.vertx = vertx;
        this.options = new OpenTsDbOptions(options);

        if(this.options.getDefaultTagCount() == 0) {
            logger.warn("You have no default tags set this is going to be an issue, opentsdb requires at last one tag on each data point (i.e. host=your.host.com)");
        }
    }

    @Override
    public void verticleDeployed(Verticle verticle) {}

    @Override
    public void verticleUndeployed(Verticle verticle) {}

    @Override
    public void timerCreated(long id) {}

    @Override
    public void timerEnded(long id, boolean cancelled) {}

    @Override
    public EventBusMetrics createMetrics(EventBus eventBus) {
        // for some reason the event bus is null on the vertx object, the one passed in will be the one on the vertx
        //  instance eventually but it would be good to not have to have the 2 constructors on ScheduleMetrics
        EventBusMetricsImpl metrics =  new EventBusMetricsImpl(this.options, vertx, eventBus);

        if (options.isEventBusEnabled()) { metrics.schedule(); }
        return metrics;
    }

    @Override
    public HttpServerMetrics<?, ?, ?> createMetrics(HttpServer server, SocketAddress localAddress, HttpServerOptions options) {
        HttpServerMetricsImpl metrics =  new HttpServerMetricsImpl(this.options, vertx).schedule();

        if(this.options.isHttpServerEnabled()) { metrics.schedule(); }
        return metrics;
    }

    @Override
    public HttpClientMetrics<?, ?, ?> createMetrics(HttpClient client, HttpClientOptions options) {
        HttpClientMetricsImpl metrics =  new HttpClientMetricsImpl(this.options, vertx).schedule();

        if(this.options.isHttpClientEnabled()) { metrics.schedule(); }
        return metrics;
    }

    @Override
    public TCPMetrics<?> createMetrics(NetServer server, SocketAddress localAddress, NetServerOptions options) {
        TcpMetricsImpl metrics = new TcpMetricsImpl(this.options, vertx, true);

        if(this.options.isTcpServerEnabled()) { metrics.schedule(); }
        return metrics;
    }

    @Override
    public TCPMetrics<?> createMetrics(NetClient client, NetClientOptions options) {
        TcpMetricsImpl metrics = new TcpMetricsImpl(this.options, vertx, false);

        if(this.options.isTcpClientEnabled()) { metrics.schedule(); }
        return metrics;
    }

    @Override
    public DatagramSocketMetrics createMetrics(DatagramSocket socket, DatagramSocketOptions options) {
        // currently not implemented
        return new DatagramSocketMetricsImpl(this.options, vertx);
    }

    @Override
    public boolean isMetricsEnabled() { return true; }

    @Override
    public boolean isEnabled() { return true; }

    @Override
    public void close() {}
}
