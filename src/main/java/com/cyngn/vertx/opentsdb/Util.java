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


import io.vertx.core.json.JsonObject;

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
}
