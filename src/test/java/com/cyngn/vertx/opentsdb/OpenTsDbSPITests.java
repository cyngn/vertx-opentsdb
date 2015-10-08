package com.cyngn.vertx.opentsdb;

import com.cyngn.vertx.opentsdb.service.OpenTsDbService;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.Deployment;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.WebTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * An integration tests to test to verify full SPI integration.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/15/15
 */
@Ignore("Integration tests, comment out annotation to run the tests with debug logging on you will see the raw metrics written")
@RunWith(VertxUnitRunner.class)
public class OpenTsDbSPITests extends WebTestBase {

    private static String topic = "test-opentsdb";
    private JsonObject config;

    public OpenTsDbSPITests() {
        config = new JsonObject();
        config.put("address", topic);
        JsonArray array = new JsonArray();
        array.add(new JsonObject().put("host", "localhost").put("port", 4242));
        config.put("hosts", array);
        config.put("max_tags", 1);
        config.put("tags", new JsonObject().put("host", "test.host.com"));
    }

    @Before
    public void before(TestContext context) {
        Async async = context.async();
        vertx.deployVerticle(OpenTsDbService.class.getName(), new DeploymentOptions().setConfig(config),
            result -> {
                if (!result.succeeded()) {
                    result.cause().printStackTrace();
                    context.fail(result.cause());
                }
                async.complete();
            });
    }

    @After
    public void after(TestContext context) {
        Deployment deployment = ((VertxInternal) vertx).getDeployment(vertx.deploymentIDs().iterator().next());
        vertx.undeploy(deployment.deploymentID());
    }

    @Override
    public VertxOptions getOptions() {
        return (new VertxOptions().setMetricsOptions(new OpenTsDbOptions(config).setEnabled(true)));
    }

    @Test
    public void testWebRequest(TestContext context) throws Exception {
        // create a handler that listens for all things
        router.route().handler(rc -> rc.response().end());

        Async async = context.async();
        testRequest(HttpMethod.GET, "/", null, resp -> { }, 200, "OK", null);
        testRequest(HttpMethod.GET, "/foo", null, resp -> { }, 200, "OK", null);
        testRequest(HttpMethod.GET, "/bar", null, resp -> {}, 200, "OK", null);

        vertx.setTimer(10000, timer -> async.complete());
    }
}
