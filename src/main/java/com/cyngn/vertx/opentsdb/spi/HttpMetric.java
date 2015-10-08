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

import com.cyngn.vertx.opentsdb.Util;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

import java.util.Objects;
import java.util.Queue;
import java.util.StringTokenizer;

/**
 * Handles generically tracking Http metric data.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/7/15
 */
public class HttpMetric {

    public static final String DELIMITER = ".";

    final SocketMetric socketMetric;
    final HttpMethod method;
    final String metricName;
    final long start;
    private final String bytesReadMetric;
    private final String bytesWrittenMetric;

    public HttpMetric(SocketMetric socketMetric, HttpMethod method, String uri) {
       this(socketMetric, method, uri, false);
    }

    public HttpMetric(SocketMetric socketMetric, HttpMethod method, String uri, boolean clientMetric) {
        this.socketMetric = socketMetric;
        this.method = method;
        this.metricName = getBaseMetricName(uri, method.name(), clientMetric);
        this.start = System.currentTimeMillis();

        bytesReadMetric = metricName + DELIMITER + "bytes_read";
        bytesWrittenMetric = metricName + DELIMITER + "bytes_written";
    }

    /**
     * Generates a base metric string from an api in a friendly format.
     *
     * @param apiName The API path, e.g. /api/v1/endpoint
     * @param httpMethod The HTTP method, e.g. GET or POST
     * @param clientReq is this a request from a http client
     * @return A formatted lower case metric name, e.g. api.v1.endpoint.get
     */
    private static String getBaseMetricName(String apiName, String httpMethod, boolean clientReq) {
        Objects.requireNonNull(apiName);

        StringTokenizer tokenizer = new StringTokenizer(apiName, "/");

        String baseMetric = "http" + DELIMITER;
        if (clientReq) { baseMetric = "http_client" + DELIMITER; }

        while(tokenizer.hasMoreElements()) { baseMetric += tokenizer.nextToken() + DELIMITER; }

        return (baseMetric + httpMethod).toLowerCase();
    }

    public void queueMetrics(Queue<JsonObject> queue, int httpStatus) {
        long end = System.currentTimeMillis();

        queue.add(Util.createRawMetric(metricName + DELIMITER + httpStatus, (end - start) + ""));
        Util.addMetricIfNonZero(queue, bytesReadMetric, socketMetric.bytesRead);
        Util.addMetricIfNonZero(queue, bytesWrittenMetric, socketMetric.bytesWritten);
    }
}
