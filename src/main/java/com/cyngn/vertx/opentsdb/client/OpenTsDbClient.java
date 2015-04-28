package com.cyngn.vertx.opentsdb.client;

import com.cyngn.vertx.opentsdb.EventBusMessage;
import com.cyngn.vertx.opentsdb.OpenTsDbReporter;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Client for communicating directly with OpenTsDb.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 4/18/15
 */
public class OpenTsDbClient implements MetricsSender, Closeable {

    private final String host;
    private final int port;
    private final Vertx vertx;
    private final NetClient netClient;
    private final EventBus bus;
    private Logger logger = LoggerFactory.getLogger(OpenTsDbClient.class);

    private Buffer readData;

    private NetSocket connection;

    private long initialBackOffMilli = 1000;
    private long lastTimeout = initialBackOffMilli;
    private long maxReconnectTime = 64000;

    private boolean connected;
    private int consecutiveDisconnects;
    private long bytesWrittenForPeriod;
    private int errorsReceived;

    public OpenTsDbClient(String host, int port, Vertx vertx, Consumer<Boolean> onInitialized) {
        NetClientOptions options = new NetClientOptions()
                .setTcpKeepAlive(true);
        netClient = vertx.createNetClient(options);
        connected = false;
        consecutiveDisconnects = 0;
        bytesWrittenForPeriod = 0;
        bus = vertx.eventBus();

        this.vertx = vertx;

        this.host = host;
        this.port = port;

        readData = Buffer.factory.buffer();

        netClient.connect(port, host, new AsyncResultHandler<NetSocket>() {
            @Override
            public void handle(AsyncResult<NetSocket> connectResult) {
                onInitialized.accept(connectResult.succeeded());
                if (connectResult.succeeded()) {
                   onConnect(connectResult.result());
                } // if we don't succeed initially we'll fail startup of the reporter
            }
        });
    }

    private void initializeHandlers() {
        connection.closeHandler(this::onClose);
        connection.handler(this::onDataReceived);
        connection.endHandler(this::onReadStreamClosed);
        connection.exceptionHandler(this::onException);
    }

    private void onException(Throwable throwable) {
        logger.error(String.format("Got exception on socket %s, ex: ", connection.remoteAddress()), throwable);
        close();
        processReconnect();
    }

    private long processReconnect() {
        long reconnectIn = lastTimeout < maxReconnectTime ? lastTimeout * 2 : maxReconnectTime;
        lastTimeout = reconnectIn;

        // attempt reconnect after back off time
        vertx.setTimer(reconnectIn, timer -> reconnect());
        return reconnectIn;
    }

    private void onConnect(NetSocket connection) {
        logger.info(String.format("Connected to host: %s port: %d", host, port));
        this.connection = connection;
        lastTimeout = initialBackOffMilli;
        initializeHandlers();
        connected = true;
    }

    private void reconnect() {
        logger.info(String.format("Reconnecting to host: %s port: %d", host, port));
        netClient.connect(port, host, new AsyncResultHandler<NetSocket>() {
            @Override
            public void handle(AsyncResult<NetSocket> connectResult) {
                if (connectResult.succeeded()) {
                    onConnect(connectResult.result());
                } else {
                    long reconnectIn = processReconnect();
                    logger.info(String.format("Failed to connect to host: %s port: %d, will re-attempt in %d(ms)", host,
                            port, reconnectIn));
                }
            }
        });
    }

    private void onReadStreamClosed(Void aVoid) {
        logger.warn("Read streamed closed");
        close();
        processReconnect();
    }

    /**
     * Reading data when you haven't sent a command in OpenTsDb means there are errors coming back from an agent
     *
     * @param buffer
     */
    private void onDataReceived(Buffer buffer) {
        logger.error("Got data from agent: " + buffer.toString(StandardCharsets.UTF_8.toString()) + " this is not expected");
        // let the user know if they failed to write because the data is invalid
        bus.send(OpenTsDbReporter.ERROR_MESSAGE_ADDRESS, new JsonObject().put("error", EventBusMessage.INVALID_DATA));
        errorsReceived++;
    }

    public boolean write(Buffer metricData) {
        if(connection.writeQueueFull()) {
            logger.error(String.format("Discarding %d bytes write buffer full", metricData.length()));
            return false;
        } else if (!connected) {
            logger.error(String.format("Discarding %d bytes no connection", metricData.length()));
            return false;
        }
        connection.write(metricData);
        bytesWrittenForPeriod += metricData.length();
        return true;
    }

    private void onClose(Void aVoid) {
        logger.info(String.format("Closing socket: %s", connection.remoteAddress()));
        connected = false;
        consecutiveDisconnects++;
    }

    public boolean isConnected() {
        return connected;
    }

    public void dumpStats() {
        int tmpDisconnects = consecutiveDisconnects;
        consecutiveDisconnects = 0;
        long tmpBytes = bytesWrittenForPeriod;
        bytesWrittenForPeriod = 0;
        int tmpErrorsReceived = errorsReceived;
        errorsReceived = 0;

        logger.info(String.format("host: %s port: %d disconnects: %d bytesWritten: %d, errorsReceived: %d", host, port,
                tmpDisconnects, tmpBytes, tmpErrorsReceived));
    }

    public void close() {
        if(isConnected()) {
            connection.close();
        }
    }
}
