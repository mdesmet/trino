/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.client.QueryResults;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.tpch.TpchTable;
import org.assertj.core.util.Files;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.KILL_QUERY;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.tracing.TracingJsonCodec.tracingJsonCodecFactory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestArrowQueryResource
{
    private static final JsonCodec<List<BasicQueryInfo>> BASIC_QUERY_INFO_CODEC = tracingJsonCodecFactory().listJsonCodec(BasicQueryInfo.class);

    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeMethod
    public void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    public void testIdempotentResults()
    {
        String sql = "SELECT returnflag FROM tpch.tiny.lineitem limit 10";

        Request request = preparePost()
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setHeader(TRINO_HEADERS.requestClientCapabilities(), "ARROW_RESULTS")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build())
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        URI uri = queryResults.getNextUri();
        ArrayList<@Nullable Object> datas = Lists.newArrayList();

        while (uri != null) {
            QueryResults results = client.execute(
                    prepareGet()
                            .setHeader(TRINO_HEADERS.requestUser(), "user")
                            .setHeader(TRINO_HEADERS.requestClientCapabilities(), "ARROW_RESULTS")
                            .setUri(uri)
                            .build(),
                    createJsonResponseHandler(jsonCodec(QueryResults.class)));
            // if (results.getData() != null && results.getData().iterator().hasNext()
            // ) {
            //     List<Object> dataItem = results.getData().iterator().next();
            //     if (!dataItem.isEmpty()) {
            //             System.out.println("bang!");
            //         HashMap firstItem = (HashMap)dataItem.get(0);
            //         if (!firstItem.isEmpty()) {
            //         }
            //     }
            // }

            Iterable<List<Object>> data = results.getData();
            if (data != null){
                datas.add(data);
            }

            uri = results.getNextUri();
        }
        System.out.println(datas);
    }

    public static final class ArrowQueryRunnerMain
    {
        private ArrowQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            TestingTrinoServer server = TestingTrinoServer.create();
            server.installPlugin(new TpchPlugin());
            server.createCatalog("tpch", "tpch");
            Logger log = Logger.get(TestArrowQueryResource.class);
            log.info("======== SERVER STARTED ========");
        }
    }
}
