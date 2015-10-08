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
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpServerMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tracks HttpServer metrics.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/8/15
 */
public class HttpServerMetricsImpl extends HttpMetricsImpl implements HttpServerMetrics<HttpMetric, Void, SocketMetric> {

    private LinkedBlockingQueue<JsonObject> requests = new LinkedBlockingQueue<>();

    public HttpServerMetricsImpl(OpenTsDbOptions options, Vertx vertx) {
        super(options, vertx, true);
    }

    @Override
    public HttpServerMetricsImpl schedule() { return (HttpServerMetricsImpl) super.schedule(); }

    @Override
    public void collectMetrics(List<JsonObject> metrics) {
        super.collectMetrics(metrics);
        List<JsonObject> list = new ArrayList<>(requests.size());
        requests.drainTo(list);
        for(int i = 0; i < list.size(); i++) { metrics.add(list.get(i)); }
    }

    @Override
    public HttpMetric requestBegin(SocketMetric socketMetric, HttpServerRequest request) {
        socketMetric.bytesRead = 0;
        socketMetric.bytesWritten = 0;
        return new HttpMetric(socketMetric, request.method(), request.uri());
    }

    @Override
    public void responseEnd(HttpMetric requestMetric, HttpServerResponse response) {
        requestMetric.queueMetrics(requests, response.getStatusCode());
    }

    @Override
    public Void upgrade(HttpMetric requestMetric, ServerWebSocket serverWebSocket) {
        return null;
    }

    @Override
    public Void connected(SocketMetric socketMetric, ServerWebSocket serverWebSocket) {
        super.connected(serverWebSocket.remoteAddress());
        return null;
    }

    @Override
    public void disconnected(Void serverWebSocketMetric) {
        // nothing to do here
    }

    @Override
    public void disconnected(SocketMetric socketMetric, SocketAddress remoteAddress) {
        super.disconnected(socketMetric, remoteAddress);
    }
}
