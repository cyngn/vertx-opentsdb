# Mod OpenTsDb

This module allows you to save metrics into an OpenTsDb cluster. Which is a time series db for tracking service and business metrics.

####To use this module you must have an OpenTsDb instance running on your network.

This is a multi-threaded worker module. The module keeps a dedicated socket connection to every OpenTsDb endpoint you configure. As metrics come in the work is then split between dedicated workers. In most cases you will probably only have one endpoint and one worker. 

## Name

The module name is `mod-opentsdb`

## Configuration

The mod-opentsdb module takes the following configuration:

    {
        "address" : <address>,
        "hosts" : [{"host" : <host1>, "port" : <host1Port>}, {"host" : <host2>, "port" : <host2Port>}],
        "maxBufferSizeInBytes" : <default 1500>,
        "prefix" : <prefix>,
        "tags" : { "key1" : "value1", "key2" : "value2"},
        "maxTags" : <default 8>
    }

For example:

    {
        "address" : "opentsdb-metrics",
        "hosts" : [{"host" : "localhost", "port" : 4242}],
        "prefix" : "myTestService",
        "tags" : { "host" : "mytesthost.com", "service" : "myTestService", "region" : "us-west1"},
        "maxTags" : 4
    }

Field breakdown:

* `address` The main address for the module. Every module has a main address. Defaults to `vertx.opentsdb-reporter"`.
* `hosts` A list of hosts that represent your OpenTsdb cluster, defaults to a list of one pointing at localhost:4242, in a multiple hosts setup a dedicated worker connection will be associated per host.
* `maxBufferSizeInBytes` The max bytes to send in any send to OpenTsDb, defaults to MTU of 1500 bytes.
* `prefix` The prefix to pre-pend to all metrics, defaults to nothing. If you set it it will add "[yourPrefix]." to all your metrics.
* `tags` The map of tags to send a long by default with all metrics. These are tags you would always want associated with every metric your service is publishing the default is to have no default tags.
* `maxTags` The max number of tags that the OpenTsdb is configured to handle.  By default, OpenTsdb instances can handle 8, thus we use it as the default here.  If you increase it, make sure all of your OpenTsdb instances have been configured correctly.

## Operations

### Add

Adds a metric to be sent to OpenTsDb

To add a metric send a JSON message to the module main address:

    {
        "action" : "add",
        "name" : <metricName>,
        "value" : <metricValue>,
        "tags" : { "key1" : "value1", 
                    "key2" : "value2" 
                 }
    }
    
Where: 

* `name` is the metric name to add to open tsdb, ie 'api.add_item.time'
* `value` the timing data for metric in this example '150.23'
* `tags` : an optional map of tags to send with just this metric being added

An example:

    {
        "action" : "add",
        "name" : "api.add_item.time",
        "value" : "150.23",
        "tags" : {"type" : "t"}
    }
    
When the add completes successfully (which can be ignored), a reply message is sent back to the sender with the following data:

    {
        "status": "ok"
    }
    
If an error occurs in adding the metric you will get back a message with:

    {
        "status": "error",
        "message": <message>
    }

Where

* `message` is an error message.

