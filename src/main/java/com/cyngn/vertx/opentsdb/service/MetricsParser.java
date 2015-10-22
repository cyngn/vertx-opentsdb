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
package com.cyngn.vertx.opentsdb.service;

import com.cyngn.vertx.opentsdb.Util;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.function.BiConsumer;

/**
 * Handles constructing proper OpenTsDb metric strings.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/11/14
 */
public class MetricsParser {

    public static String NAME_FIELD = "name";
    public static String VALUE_FIELD = "value";
    public static String TAGS_FIELD = "tags";
    public static String METRICS_FIELD = "metrics";

    private final BiConsumer<Message<JsonObject>, String> errorHandler;
    private final String defaultTags;
    private final String prefix;
    private boolean hasPrefix;

    /**
     *
     * @param prefix the prefix to put on all metrics
     * @param defaultTags the default tags to put on all metrics
     * @param errorHandler the error handler to use to notify the event bus sender of failures
     */
    public MetricsParser(String prefix, String defaultTags, BiConsumer<Message<JsonObject>, String> errorHandler) {
        this.prefix = prefix;
        hasPrefix = prefix != null && prefix.length() > 0;
        this.defaultTags = defaultTags == null ? "" : defaultTags;
        this.errorHandler = errorHandler;
    }

    /**
     * Given a event bus message take the metric data from it and create an OpenTsDb string to send to OpenTsDb
     *
     * @param message the event bus message
     * @param metric the metric object
     * @return the metric string or null if it is invalid
     */
    @SuppressWarnings("unchecked")
    public String createMetricString(Message message, JsonObject metric) {
        String metricName = metric.getString(NAME_FIELD, "");
        if (StringUtils.isEmpty(metricName)) {
            errorHandler.accept(message, "All metrics need a 'name' field");
            return null;
        }

        String metricValue = metric.getString(VALUE_FIELD, "");
        if (metricValue.length() == 0) {
            errorHandler.accept(message, "All metrics need a 'value' field");
            return null;
        }

        String tags = getTagString(metric.getJsonObject(TAGS_FIELD));

        // this is an OpenTsDB requirement
        if (StringUtils.isEmpty(tags.trim())) {
            errorHandler.accept(message, "You must specify at least one tag");
            return null;
        }

        return getMetricString(metricName, metricValue, tags);
    }

    /**
     * Given the raw metric data create an OpenTsDb formatted string.
     *
     * @param name the metric name
     * @param value the metric value
     * @param tags the tags string for this data point
     * @return an OpenTsDb metric string
     */
    public String getMetricString(String name, String value, String tags) {
        long timestamp = DateTime.now(DateTimeZone.UTC).toDate().getTime();
        return (hasPrefix) ? String.format("put %s.%s %d %s %s\n", prefix, name, timestamp, value, tags)
                           : String.format("put %s %d %s %s\n", name, timestamp, value, tags);
    }

    /**
     * Get a string representing the tags to send with the OpenTsDb metric
     *
     * @param tags the tags map
     * @return a string representing all tags
     */
    public String getTagString(JsonObject tags) {
        String tagsStr = defaultTags;
        if (tags != null) {
            String padding = tagsStr.equals("") ? "" : " ";
            tagsStr += padding + Util.createTagsFromJson(tags);
        }

        return tagsStr;
    }
}
