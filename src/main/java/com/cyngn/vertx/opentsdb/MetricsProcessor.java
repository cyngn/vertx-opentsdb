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

import com.cyngn.vertx.opentsdb.client.MetricsSender;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Handles chunking metric data into optimal sizes to OpenTsdb
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class MetricsProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MetricsProcessor.class);
    private final List<MetricsSender> metricsSenders;
    private final int maxBufferSizeInBytes;
    private final EventBus bus;


    public MetricsProcessor(List<MetricsSender> metricsSenders, int maxBufferSizeInBytes, EventBus bus) {
        this.metricsSenders = metricsSenders;
        this.maxBufferSizeInBytes = maxBufferSizeInBytes;
        this.bus = bus;
    }

    /**
     * Given a queue of metrics to send, process the metrics into the right format and send them over a socket
     *
     * @param metrics the metrics queue to work off
     */
    public void processMetrics(LinkedBlockingQueue<String> metrics) {
        int metricCount = metrics.size();
        if (metricCount == 0) {return;}
        List<String> drainedMetrics = new ArrayList<>();

        metrics.drainTo(drainedMetrics);
        Buffer outputBuffer = Buffer.buffer();

        int senderPos = 0;
        MetricsSender currentSender = metricsSenders.get(senderPos);

        int nextRotateIndex = drainedMetrics.size() / metricsSenders.size();
        int switchInterval = nextRotateIndex + 1;

        // loop through and serialize the metrics and send them as we fill the buffer up to max buffer
        for (int i = 0; i < drainedMetrics.size(); i++) {
            // ponder if one of the host is disconnected
            if (i == nextRotateIndex) {
                // flush the current remaining data queued before moving to the next sender
                outputBuffer = write(currentSender, outputBuffer);

                senderPos++;
                currentSender = metricsSenders.get(senderPos);
                nextRotateIndex += switchInterval;
            }

            String metric = drainedMetrics.get(i);
            byte[] bytes = metric.getBytes();

            // if this would exceed the max buffer to send go ahead and pass to the sender
            if (bytes.length + outputBuffer.length() > maxBufferSizeInBytes) {
                outputBuffer = write(currentSender, outputBuffer);
            }

            outputBuffer.appendBytes(bytes);
        }

        // send whatever is left in the buffer
        if (outputBuffer.length() > 0) {
            write(currentSender, outputBuffer);
        }
    }

    private Buffer write(MetricsSender sender, Buffer data) {
        boolean success = sender.write(data);
        if(!success) {
            bus.send(OpenTsDbReporter.ERROR_MESSAGE_ADDRESS, new JsonObject().put("error",
                    EventBusMessage.WRITE_FAILURE.toString()));
        }

        return Buffer.buffer();
    }
}
