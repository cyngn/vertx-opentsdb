
[![Build Status](https://travis-ci.org/cyngn/vertx-opentsdb.svg?branch=master)](https://travis-ci.org/cyngn/vertx-opentsdb)

# vert.x OpenTsDb

This library allows you to save metrics into an OpenTsDb cluster. Which is a time series db for tracking service and business metrics.

#### To use this library you must have an OpenTsDb instance running on your network.

The library keeps 1 dedicated NIO socket connection to every OpenTsDb endpoint you configure. As metrics come they are flushed on a configurable timer schedule and the work is then split between the dedicated connections. In most cases you will probably only have one endpoint and one worker.

## Getting Started

Add a dependency to vertx-opentsdb:

```xml
<dependency>
    <groupId>com.cyngn.vertx</groupId>
    <artifactId>vertx-opentsdb</artifactId>
    <version>3.3.0-SNAPSHOT</version>
</dependency>
```

## Configuration

The vertx-opentsdb module takes the following configuration:

```json
    {
        "auto_deploy_verticle" : <default true>,
        "address" : <address>,
        "hosts" : [{"host" : <host1>, "port" : <host1Port>}, {"host" : <host2>, "port" : <host2Port>}],
        "max_buffer_bytes" : <default 1500>,
        "prefix" : <prefix>,
        "tags" : { "key1" : "value1", "key2" : "value2"},
        "max_tags" : <default 8>,
        "flush_interval_milli" : <default 1000>,
        "max_backlog" : <default INFINITE>,
        "spi_publish_interval" : <default 1000>
    }
```

For example:
```java
{
    "address" : "opentsdb-metrics",
    "hosts" : [{"host" : "localhost", "port" : 4242}],
    "prefix" : "myTestService",
    "tags" : { "host" : "mytesthost.com", "service" : "myTestService", "region" : "us-west1"},
    "max_tags" : 4
}
```

Field breakdown:

* `auto_deploy_verticle` In the scenario where you are using the VertxMetrics API do you want to automatically have it deploy the OpenTsDb verticle or handle it yourself
* `address` The main address for the module. Every module has a main address. Defaults to `vertx.opentsdb-reporter"`.
* `hosts` A list of hosts that represent your OpenTsdb cluster, defaults to a list of one pointing at localhost:4242, in a multiple hosts setup a dedicated worker connection will be associated per host.
* `max_buffer_bytes` The max bytes to send in any send to OpenTsDb, defaults to MTU of 1500 bytes.
* `prefix` The prefix to pre-pend to all metrics, defaults to nothing. If you set it it will add "[yourPrefix]." to all your metrics.
* `tags` The map of tags to send a long by default with all metrics. These are tags you would always want associated with every metric your service is publishing the default is to have no default tags.
* `max_tags` The max number of tags that the OpenTsdb is configured to handle.  By default, OpenTsdb instances can handle 8, thus we use it as the default here.  If you increase it, make sure all of your OpenTsdb instances have been configured correctly.
* `flush_interval_milli` How frequently in milliseconds to flush queued reported metrics out to Open TsDb. This defaults to once a second.
* `max_backlog` The maximum number of metrics to allow to be queued between flush intervals, this defaults to an unlimited amount.
* `spi_publish_interval` The frequency in milliseconds to publish SPI metrics to OpenTsDb

## Operations

### Add

Adds a metric to be sent to OpenTsDb

To add a metric send a JSON message to the module main address:

```json
{
    "action" : "add",
    "name" : <metricName>,
    "value" : <metricValue>,
    "tags" : { "key1" : "value1",
               "key2" : "value2"
     }
}
```

Where:

* `name` is the metric name to add to open tsdb, ie 'api.add_item.time'
* `value` the timing data for metric in this example '150.23'
* `tags` : an optional map of tags to send with just this metric being added

An example:

```json
{
    "action" : "add",
    "name" : "api.add_item.time",
    "value" : "150.23",
    "tags" : {"type" : "t"}
}
```

When the add completes successfully (which can be ignored), a reply message is sent back to the sender with the following data:

        "ok"

If an error occurs when adding the metric you will get back a response as failed and you need to check the 'cause' method for the issue, ie

```java
JsonObject metric = Util.createMetric("test.value", "34.4", new JsonObject().put("host", "test.host.com"));

eventBus.send(topic, metric, new DeliveryOptions(), new Handler<AsyncResult<Message<JsonObject>>>() {
  @Override
  public void handle(AsyncResult<Message<JsonObject>> result) {
    if(result.failed()) {
      System.out.println("Got error ex: ", result.cause())
    }
  }
});
```
### Add All

Adds a list of metrics to be sent to OpenTsDb

To add metrics send a JSON message to the module main address:

```json
{
    "action" : "add_all",
    "metrics" : [{
      "name" : <metricName>,
      "value" : <metricValue>,
      "tags" : {
        "key1" : "value1",
        "key2" : "value2"
      },
      {
        "name" : <metricName>,
        "value" : <metricValue>,
        "tags" : {
          "key1" : "value1",
          "key2" : "value2"
        }
   }]
}
```

Where:

* `metrics` is a JsonArray of metric objects

## Generic Metric Publisher

There is a publisher that is provided to assist you in publishing metrics to the OpenTsDb verticle. It has a lot of helper functions you just need to give it an event bus reference and your vertx-opentsdb listening topic.

Example:

```java
MetricPublisher publisher = new MetricPublisher(bus, "vertx-opentsdb-topic")

publisher.send("error.count", 5);
publisher.send("error.count", 10, new JsonObject().put("ui.screen", "landing_page"))

List<TsMetric> list = new ArrayList<>();
list.add(new TsMetric("ui.loaded", 5))
list.add(new TsMetric("ui.unloaded", 7))
publisher.sendTsMetricBatch(list);
```

## SPI Integration

The library supports integration with the Vert.x SPI metrics interfaces so you can automatically publish metrics related to things that the vert.x framework is doing like http calls and event bus messages. You can enable this support like so:

```java
// you need to initialize your vert.x instance with the metrics options turned on
Vertx.vertx(new VertxOptions().setMetricsOptions(new OpenTsDbOptions(config).setEnabled(true)));
```

## Event Bus Error Messages

In some, hopefully rare cases, you may need listen to messages on the event bus for notifications of errors happening asynchronously that unmonitored could result in an extended loss of metrics if not dealt with.

The topic you need to listen on is '**vertx-opentsdb-errors**', the message you receive will come in the following form:

```java
{
   "error" : "<detailed error message>"
}
```
#### errors

* ***WRITE_FAILURE*** - the library is failing to write on the socket, check the logs for detailed errors
* ***INVALID_DATA*** - Open TsDb is reporting back to the library that invalidly formatted data is being submitted or there is some other error an agent is encountering

#### Example code
```java
eventBus.consumer(OpenTsDbReporter.ERROR_MESSAGE_ADDRESS, new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> event) {
        // deal with alerting
    }
});
```
