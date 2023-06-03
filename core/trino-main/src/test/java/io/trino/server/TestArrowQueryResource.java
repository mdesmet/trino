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

import com.google.common.collect.Lists;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.trino.client.QueryResults;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestArrowQueryResource
{
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
        String sql = "SELECT * FROM tpch.tiny.nation Limit 7";

        Request request = preparePost()
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setHeader(TRINO_HEADERS.requestClientCapabilities(), "ARROW_RESULTS")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build())
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));

        pollForResults(queryResults.getNextUri(), client, Lists.newArrayList());
    }

    @Test
    public void testMappingResults()
    {
        String sql = "SELECT nationkey,name,regionkey,comment FROM tpch.tiny.nation limit 10";

        Request request = preparePost()
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setHeader(TRINO_HEADERS.requestClientCapabilities(), "ARROW_RESULTS")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build())
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        URI uri = queryResults.getNextUri();
        ArrayList<Iterable<List<Object>>> chunks = Lists.newArrayList();

        pollForResults(uri, client, chunks);

        String firstChunk = chunks.get(0).iterator().next().get(0).toString();
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(firstChunk.getBytes(UTF_8)));
                ArrowStreamReader reader = new ArrowStreamReader(inputStream, new RootAllocator(Long.MAX_VALUE))) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            reader.loadNextBatch();
            root.getFieldVectors().forEach(vector -> {
                System.out.printf("getName: %s%n", vector.getField().getName());
                System.out.printf("getType: %s%n", vector.getField().getType());
                System.out.println("values:");
                for (int i = 0; i < vector.getValueCount(); i++) {
                    System.out.println(vector.getObject(i));
                }
            });

            assertEquals((root.getFieldVectors().get(0).getObject(0)).toString(), "0");
            assertEquals((root.getFieldVectors().get(1).getObject(0)).toString(), "ALGERIA");
            assertEquals((root.getFieldVectors().get(2).getObject(0)).toString(), "0");
            assertEquals((root.getFieldVectors().get(3).getObject(0)).toString(), " haggle. carefully final deposits detect slyly agai");
        }

        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void pollForResults(URI uri, HttpClient client, ArrayList<Iterable<List<Object>>> datas)
    {
        while (uri != null) {
            QueryResults results = client.execute(
                    prepareGet()
                            .setHeader(TRINO_HEADERS.requestUser(), "user")
                            .setHeader(TRINO_HEADERS.requestClientCapabilities(), "ARROW_RESULTS")
                            .setUri(uri)
                            .build(),
                    createJsonResponseHandler(jsonCodec(QueryResults.class)));

            Iterable<List<Object>> data = results.getData();
            if (data != null) {
                datas.add(data);
            }

            uri = results.getNextUri();
        }
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
            log.info(server.getBaseUrl().toString());
        }
    }

    public static final class QueryClientRunner
    {
        private static HttpClient client = new JettyHttpClient();

        private QueryClientRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            String sql = "SELECT nationkey,name,regionkey,comment FROM tpch.tiny.nation";

            Request request = preparePost()
                    .setHeader(TRINO_HEADERS.requestUser(), "user")
                    .setHeader(TRINO_HEADERS.requestClientCapabilities(), "ARROW_RESULTS")
                    .setUri(uriBuilderFrom(URI.create("http://127.0.0.1:56329").resolve("/v1/statement")).build())
                    .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                    .build();

            QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
            URI uri = queryResults.getNextUri();
            ArrayList<Iterable<List<Object>>> datas = Lists.newArrayList();

            pollForResults(uri, client, datas);
            System.out.println(datas);
        }
    }
}
