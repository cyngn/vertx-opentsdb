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
package com.cyngn.vertx.opentsdb;


import com.cyngn.vertx.opentsdb.service.MetricsParser;
import com.cyngn.vertx.opentsdb.service.OpenTsDbService;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Collection;

/**
 * General purpose utils
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class Util {

    /**
     * Take config specified tags and make a OpenTsDb tag string
     *
     * @param tags the map of tags to convert to their string form
     * @return list of tags in opentsdb format ie 'name1=value1 name2=value2'
     */
    public static String createTagsFromJson(JsonObject tags) {
        String tagsString = "";
        if (tags != null && tags.size() > 0) {
            StringBuilder builder = new StringBuilder();
            for (String key : tags.fieldNames()) {
                builder.append(key).append("=").append(tags.getString(key)).append(" ");
            }
            // if necessary, grab all but the last character, since it's always a space
            if (builder.length() > 0) {
                tagsString = builder.substring(0, builder.length() -1);
            } else {
                tagsString = builder.toString();
            }
        }

        return tagsString;
    }

    /**
     * Create a metric object for sending to the OpenTsDb lib
     *
     * @param name the metric name
     * @param value the metric value
     * @return the properly constructed metric object
     */
    public static JsonObject createMetric(String name, String value) {
        return createMetric(name, value, null);
    }

    /**
     * Create a metric object for sending to the OpenTsDb lib
     *
     * @param name the metric name
     * @param value the metric value
     * @param tags the tags to associate to this metric
     * @return the properly constructed metric object
     */
    public static JsonObject createMetric(String name, String value, JsonObject tags) {
        JsonObject obj = new JsonObject().put(OpenTsDbService.ACTION_FIELD, OpenTsDbService.ADD_COMMAND)
                .put(MetricsParser.NAME_FIELD, name).put(MetricsParser.VALUE_FIELD, value);
        if(tags != null && tags.size() > 0) {
            obj.put(MetricsParser.TAGS_FIELD, tags);
        }
        return obj;
    }

    /**
     * Create a metric object for sending to the OpenTsDb lib but with no implicit command
     *
     * @param name the metric name
     * @param value the metric value
     * @return the properly constructed metric object
     */
    public static JsonObject createRawMetric(String name, String value) {
        return createRawMetric(name, value, null);
    }

    /**
     * Create a metric object for sending to the OpenTsDb lib but with no implicit command
     *
     * @param name the metric name
     * @param value the metric value
     * @param tags the tags to associate to this metric
     * @return the properly constructed metric object
     */
    public static JsonObject createRawMetric(String name, String value, JsonObject tags) {
        JsonObject obj = new JsonObject().put(MetricsParser.NAME_FIELD, name).put(MetricsParser.VALUE_FIELD, value);
        if(tags != null && tags.size() > 0) {
            obj.put(MetricsParser.TAGS_FIELD, tags);
        }
        return obj;
    }

    /**
     * Create a bulk metric object for sending to the OpenTsDb lib
     *
     * @param name the first metric to put in the bulk requests
     * @param value the metric value to add
     * @return the properly constructed bulk metric object
     */
    public static JsonObject createBulkMetric(String name, String value) {
        return createBulkMetric(name, value, null);
    }

    /**
     * Create a bulk metric object for sending to the OpenTsDb lib
     *
     * @param name the first metric to put in the bulk requests
     * @param value the metric value to add
     * @param tags the tags to associate to this metric
     * @return the properly constructed bulk metric object
     */
    public static JsonObject createBulkMetric(String name, String value, JsonObject tags) {
        return createBulkMetric(createRawMetric(name, value, tags));
    }

    /**
     * Create a bulk metric object for sending to the OpenTsDb lib
     *
     * @param metric the first metric to put in the bulk requests
     * @return the properly constructed metric object
     */
    public static JsonObject createBulkMetric(JsonObject metric) {
        return new JsonObject().put(OpenTsDbService.ACTION_FIELD, OpenTsDbService.ADD_ALL_COMMAND)
                .put(MetricsParser.METRICS_FIELD, new JsonArray().add(metric));
    }


    /**
     * Add another metric to a bulk metric object.
     *
     * @param metricObj the bulk metric obj to add more metrics to
     * @param name the metric name to add
     * @param value the metric value to add
     * @param tags the tags to associate to this metric
     */
    public static void addBulkMetric(JsonObject metricObj, String name, String value, JsonObject tags) {
        addBulkMetric(metricObj, createRawMetric(name, value, tags));
    }

    /**
     * Add another metric to a bulk metric.
     *
     * @param bulkMetricObj the bulk metric obj to add more metrics to
     * @param metricToAdd the properly constructed individual metric object
     */
    public static void addBulkMetric(JsonObject bulkMetricObj, JsonObject metricToAdd) {
        JsonArray metricHolder = bulkMetricObj.getJsonArray(MetricsParser.METRICS_FIELD);
        if(metricHolder == null) {
            throw new IllegalArgumentException("Could not find a valid " + MetricsParser.METRICS_FIELD + " in the object");
        }

        metricHolder.add(metricToAdd);
    }

    /**
     * Adds a metric to the list IF it's for a non-zero value
     *
     * @param metrics the metric list
     * @param name the metric name
     * @param value the metric value
     */
    public static void addMetricIfNonZero(Collection<JsonObject> metrics, String name, long value) {
        if(value > 0) {
            metrics.add(createRawMetric(name, value + ""));
        }
    }
}
