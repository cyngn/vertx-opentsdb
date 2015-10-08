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
import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.VertxMetrics;

/**
 * Handle creating a new impl of the VertxMetrics interface.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/8/15
 */
public class VertxMetricsFactoryImpl implements VertxMetricsFactory {

    @Override
    public VertxMetrics metrics(Vertx vertx, VertxOptions options) {
        MetricsOptions baseOptions = options.getMetricsOptions();
        OpenTsDbOptions metricsOptions;
        if (baseOptions instanceof OpenTsDbOptions) {
            metricsOptions = (OpenTsDbOptions) baseOptions;
        } else {
            metricsOptions = new OpenTsDbOptions(baseOptions.toJson());
        }
        return new VertxMetricsImpl(metricsOptions, vertx);
    }
}
