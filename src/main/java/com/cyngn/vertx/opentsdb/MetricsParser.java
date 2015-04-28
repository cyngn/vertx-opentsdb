/*
 * Copyright 2014 Cyanogen Inc.
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
package com.cyngn.vertx.opentsdb;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.function.BiConsumer;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/11/14
 */
public class MetricsParser {

    public static String NAME_FIELD = "name";
    public static String VALUE_FIELD = "value";
    public static String TAGS_FIELD = "tags";

    private final BiConsumer<Message<JsonObject>, String> errorHandler;
    private final String defaultTags;
    private final String prefix;
    private boolean hasPrefix;

    public MetricsParser(String prefix, String defaultTags, BiConsumer<Message<JsonObject>, String> errorHandler) {
        this.prefix = prefix;
        hasPrefix = prefix != null && prefix.length() > 0;
        this.defaultTags = defaultTags == null ? "" : defaultTags;
        this.errorHandler = errorHandler;
    }

    public String createMetricString(Message<JsonObject> message) {
        JsonObject body = message.body();

        String metricName = body.getString(NAME_FIELD, "");
        if (metricName.length() == 0) {
            errorHandler.accept(message, "All metrics need a 'name' field");
            return null;
        }

        String metricValue = body.getString(VALUE_FIELD, "");
        if (metricValue.length() == 0) {
            errorHandler.accept(message, "All metrics need a 'value' field");
            return null;
        }

        String tags = defaultTags;
        if (body.containsKey(TAGS_FIELD)) {
            String padding = tags.equals("") ? "" : " ";
            tags += padding + Util.createTagsFromJson(body.getJsonObject(TAGS_FIELD));
        }

        // this is an OpenTsDB requirement
        if ("".equals(tags.trim())) {
            errorHandler.accept(message, "You must specify at least one tag");
            return null;
        }

        String metric = (hasPrefix) ?
                String.format("put %s.%s %d %s %s\n", prefix, metricName,
                        DateTime.now(DateTimeZone.UTC).toDate().getTime(), metricValue, tags)
                : String.format("put %s %d %s %s\n", metricName, DateTime.now(DateTimeZone.UTC).toDate().getTime(),
                metricValue, tags);

        return metric;
    }
}
