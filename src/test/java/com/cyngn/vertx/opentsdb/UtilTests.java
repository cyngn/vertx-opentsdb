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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class UtilTests {

    @Test
    public void testTags() {
        String tags = Util.createTagsFromJson(new JsonObject().put("foo", "bar").put("bar", "foo"));
        assertEquals(tags, "foo=bar bar=foo");
    }

    @Test
    public void testNoTags() {
        String tags = Util.createTagsFromJson(new JsonObject());
        assertEquals(tags, "");
    }
}
