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
package com.cyngn.vertx.opentsdb.client;

import com.cyngn.vertx.opentsdb.service.MetricsParser;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * A time series metric.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/15/15
 */
public class TsMetric {
    public final String name;
    public final String value;
    public final HashMap<String, String> tags;

    public TsMetric(String name, String value) {
        this(name, value, null);
    }

    public TsMetric(String name, String value, HashMap<String,String> tags) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("A metric must have a name");
        }

        if (StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("A metric must have a value");
        }

        this.name = name;
        this.value = value;
        this.tags = tags;
    }

    /**
     * Convert this object to a metric JsonObject suitable for transferring via the EventBus.
     *
     * @return the TsMetric in JsonObject form
     */
    public JsonObject asJson() {
        JsonObject jsonObject = new JsonObject().put(MetricsParser.NAME_FIELD, name)
                .put(MetricsParser.VALUE_FIELD, value);

        if (tags != null && tags.size() > 0) {
            JsonObject tagMap = new JsonObject();
            for(Map.Entry<String,String> entry : tags.entrySet()) {
                tagMap.put(entry.getKey(), entry.getValue());
            }

            jsonObject.put(MetricsParser.TAGS_FIELD, tagMap);
        }

        return jsonObject;
    }

    /**
     * All metrics need a name and value field set.
     *
     * @param jsonObject the object to validate
     * @return true if metric has the minimum data false otherwise
     */
    public static boolean isValid(JsonObject jsonObject) {
        return jsonObject.containsKey(MetricsParser.NAME_FIELD) &&
               jsonObject.containsKey(MetricsParser.VALUE_FIELD);
    }
}
