package com.cyngn.vertx.opentsdb;

import com.cyngn.vertx.opentsdb.client.MetricPublisher;
import com.cyngn.vertx.opentsdb.client.TsMetric;
import com.cyngn.vertx.opentsdb.service.MetricsParser;
import com.cyngn.vertx.opentsdb.service.OpenTsDbService;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 10/14/15
 */
@SuppressWarnings("unchecked")
public class MetricPublisherTests {

    private EventBus bus;
    private MetricPublisher mPublisher;

    @Before
    public void setUp() {
        bus = mock(EventBus.class);
        mPublisher = new MetricPublisher(bus, "test.topic");
    }

    @Test
    public void testSimplePublish() {
        ArgumentCaptor<JsonObject> metricObj = ArgumentCaptor.forClass(JsonObject.class);

        mPublisher.send("test.foo", "34.5");

        verify(bus).send(any(String.class), metricObj.capture(), any(Handler.class));

        assertEquals("test.foo", metricObj.getValue().getString(MetricsParser.NAME_FIELD));
        assertEquals("34.5", metricObj.getValue().getString(MetricsParser.VALUE_FIELD));
        assertEquals(OpenTsDbService.ADD_COMMAND, metricObj.getValue().getString(OpenTsDbService.ACTION_FIELD));
        assertFalse(metricObj.getValue().containsKey(MetricsParser.TAGS_FIELD));
    }

    @Test
    public void testSimplePublishWithTags() {
        ArgumentCaptor<JsonObject> metricObj = ArgumentCaptor.forClass(JsonObject.class);

        JsonObject tags = new JsonObject().put("host", "fake.host.com");
        mPublisher.send("test.foo", "34.5", tags);

        verify(bus).send(any(String.class), metricObj.capture(), any(Handler.class));

        assertEquals("test.foo", metricObj.getValue().getString(MetricsParser.NAME_FIELD));
        assertEquals("34.5", metricObj.getValue().getString(MetricsParser.VALUE_FIELD));
        assertEquals(OpenTsDbService.ADD_COMMAND, metricObj.getValue().getString(OpenTsDbService.ACTION_FIELD));
        assertEquals(tags, metricObj.getValue().getJsonObject(MetricsParser.TAGS_FIELD));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testBadArgsSimplePublish() {
        mPublisher.send(new JsonObject().put(MetricsParser.NAME_FIELD, "fooy"));
    }

    @Test
    public void testBulkPublish() {
        ArgumentCaptor<JsonObject> metricObj = ArgumentCaptor.forClass(JsonObject.class);

        List<JsonObject> metrics = Arrays.asList(Util.createRawMetric("foo", "12.5"), Util.createRawMetric("bar", "789"));

        mPublisher.sendMetricBatch(metrics);

        verify(bus).send(any(String.class), metricObj.capture(), any(Handler.class));

        JsonArray arry = metricObj.getValue().getJsonArray(MetricsParser.METRICS_FIELD);

        assertEquals(2, arry.size());
        assertEquals(OpenTsDbService.ADD_ALL_COMMAND, metricObj.getValue().getString(OpenTsDbService.ACTION_FIELD));
        assertEquals("12.5", arry.getJsonObject(0).getString(MetricsParser.VALUE_FIELD));
        assertEquals("789", arry.getJsonObject(1).getString(MetricsParser.VALUE_FIELD));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBulkPublishBadMetrics() {
        List<JsonObject> metrics = Collections.singletonList(new JsonObject().put(MetricsParser.NAME_FIELD, "foo"));
        mPublisher.sendMetricBatch(metrics);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBulkPublishBadMetrics2() {
        List<JsonObject> metrics = Arrays.asList(new JsonObject().put(MetricsParser.NAME_FIELD, "foo").put(MetricsParser.VALUE_FIELD, "12.4"),
                new JsonObject().put(MetricsParser.VALUE_FIELD, "12.5"));

        mPublisher.sendMetricBatch(metrics);
    }

    @Test
    public void testTsBulkPublish() {
        ArgumentCaptor<JsonObject> metricObj = ArgumentCaptor.forClass(JsonObject.class);

        List<TsMetric> metrics = Arrays.asList(new TsMetric("foo", "12.5"), new TsMetric("bar", "789"));

        mPublisher.sendTsMetricBatch(metrics);

        verify(bus).send(any(String.class), metricObj.capture(), any(Handler.class));

        JsonArray arry = metricObj.getValue().getJsonArray(MetricsParser.METRICS_FIELD);

        assertEquals(2, arry.size());
        assertEquals(OpenTsDbService.ADD_ALL_COMMAND, metricObj.getValue().getString(OpenTsDbService.ACTION_FIELD));
        assertEquals("12.5", arry.getJsonObject(0).getString(MetricsParser.VALUE_FIELD));
        assertEquals("789", arry.getJsonObject(1).getString(MetricsParser.VALUE_FIELD));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTsBulkPublishEmpty() {
        List<TsMetric> metrics = Collections.emptyList();
        mPublisher.sendTsMetricBatch(metrics);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBulkPublishEmpty() {
        List<JsonObject> metrics = Collections.emptyList();
        mPublisher.sendMetricBatch(metrics);
    }


}
