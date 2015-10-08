package com.cyngn.vertx.opentsdb;

import com.cyngn.vertx.opentsdb.spi.SpiOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.metrics.MetricsOptions;

/**
 * The options for OpenTsDb
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 9/22/15
 */
public class OpenTsDbOptions extends MetricsOptions implements SpiOptions {

    public static final int OPENTSDB_DEFAULT_MAX_TAGS = 8;
    public final static String DEFAULT_ADDRESS = "vertx.opentsdb-reporter";
    private final int DEFAULT_MTU = 1500;
    private final int defaultTagCount;
    private final String defaultTags;

    private JsonArray hosts;
    private int maxBufferBytes;
    private String address;
    private String prefix;
    private int flushInterval;
    private int maxBacklog;
    private int maxTags;
    private int spiPublishInterval;

    // flags for enabling/disabling various metrics
    private boolean eventBusEnabled = true;
    private boolean httpClientEnabled = true;
    private boolean httpServerEnabled = true;
    private boolean tcpClientEnabled = true;
    private boolean tcpServerEnabled = true;

    public OpenTsDbOptions() {
        hosts = new JsonArray("[{ \"host\" : \"localhost\", \"port\" : 4242}]");
        maxBufferBytes = DEFAULT_MTU;
        prefix = null;
        maxTags = OPENTSDB_DEFAULT_MAX_TAGS;
        address = DEFAULT_ADDRESS;
        flushInterval = 1000;
        maxBacklog = Integer.MIN_VALUE;
        defaultTagCount = 0;
        spiPublishInterval = flushInterval;
        defaultTags = "";
    }

    public OpenTsDbOptions(OpenTsDbOptions other) {
        super(other);
        address = other.address;
        hosts = other.hosts;
        maxBufferBytes = other.maxBufferBytes;
        prefix = other.prefix;
        maxTags = other.maxTags;
        flushInterval = other.flushInterval;
        maxBacklog = other.maxBacklog;
        defaultTagCount = other.defaultTagCount;
        defaultTags = other.defaultTags;
        spiPublishInterval = other.spiPublishInterval;
    }

    public OpenTsDbOptions(JsonObject config) {
        super(config);
        hosts = config.getJsonArray("hosts", new JsonArray("[{ \"host\" : \"localhost\", \"port\" : 4242}]"));
        maxBufferBytes = config.getInteger("max_buffer_bytes", DEFAULT_MTU);
        prefix = config.getString("prefix", null);
        maxTags = config.getInteger("max_tags", OPENTSDB_DEFAULT_MAX_TAGS);
        flushInterval = config.getInteger("flush_interval_milli", 1000);
        maxBacklog = config.getInteger("max_metric_backlog", Integer.MIN_VALUE);
        address = config.getString("address", DEFAULT_ADDRESS);
        spiPublishInterval = config.getInteger("spi_publish_interval", 1000);

        final JsonObject configuredTags = config.getJsonObject("tags");
        if (configuredTags != null && configuredTags.size() > maxTags) {
            throw new IllegalArgumentException("Found more default tags than the max (" + maxTags + ")");
        }
        defaultTagCount = configuredTags != null ? configuredTags.size() : 0;
        defaultTags = Util.createTagsFromJson(configuredTags);
    }

    public String getAddress() { return  address; }

    public int getDefaultTagCount() {
        return defaultTagCount;
    }

    public String getDefaultTags() {
        return defaultTags;
    }

    public JsonArray getHosts() {
        return hosts;
    }

    public int getMaxBufferBytes() {
        return maxBufferBytes;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public int getMaxBacklog() {
        return maxBacklog;
    }

    public int getMaxTags() { return maxTags; }

    public int getSpiPublishInterval() { return spiPublishInterval; }

    public boolean isEventBusEnabled() { return eventBusEnabled; }

    public OpenTsDbOptions disableEventBus() {
        eventBusEnabled = false;
        return this;
    }

    public boolean isHttpClientEnabled() { return httpClientEnabled; }

    public OpenTsDbOptions disableHttpClient() {
        httpClientEnabled = false;
        return this;
    }

    public boolean isHttpServerEnabled() { return httpServerEnabled; }

    public OpenTsDbOptions disableHttpServer() {
        httpServerEnabled = false;
        return this;
    }

    public boolean isTcpClientEnabled() { return tcpClientEnabled; }

    public OpenTsDbOptions disableTcpClient() {
        tcpClientEnabled = false;
        return this;
    }

    public boolean isTcpServerEnabled() { return tcpServerEnabled; }

    public OpenTsDbOptions disableTcpServer() {
        tcpServerEnabled = false;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("OpenTsDbOptions[maxBufferSize=").append(maxBufferBytes).append(", address=").append(address)
                .append(", maxTags=").append(maxTags).append(", flushInterval=").append(flushInterval)
                .append("(ms), maxBacklog=").append(maxBacklog == Integer.MIN_VALUE ? "INFINITY" : maxBacklog);
        if (defaultTagCount > 0) { builder.append(", tags='").append(defaultTags).append("'"); }
        builder.append(", eventBusEnabled=").append(eventBusEnabled).append(", httpClientEnabled=")
               .append(httpClientEnabled).append(", httpServerEnabled=").append(httpServerEnabled)
               .append(", tcpClientEnabled").append(tcpClientEnabled).append(", tcpServerEnabled")
               .append(tcpServerEnabled).append(", hosts='").append(hosts.encode()).append("']");

        return builder.toString();
    }

    @Override
    public OpenTsDbOptions setEnabled(boolean enable) {
        return (OpenTsDbOptions) super.setEnabled(enable);
    }
}
