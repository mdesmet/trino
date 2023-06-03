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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.trino.client.QueryResults;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.commons.codec.binary.Base64;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Streams.forEachPair;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.AssertJUnit.assertEquals;

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
    public void testSingleVarcharValue()
            throws Exception
    {
        String sql = "SELECT returnflag FROM tpch.tiny.lineitem order by 1 limit 1";
        assertArrowResult(sql, ImmutableList.of("""
                returnflag
                A
                """));
    }

    @Test
    public void testNation()
            throws Exception
    {
        String sql = "SELECT * FROM tpch.tiny.nation order by 1 limit 10";
        assertArrowResult(sql, ImmutableList.of("""
                nationkey	name	regionkey	comment
                0	ALGERIA	0	 haggle. carefully final deposits detect slyly agai
                1	ARGENTINA	1	al foxes promise slyly according to the regular accounts. bold requests alon
                2	BRAZIL	1	y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special\s
                3	CANADA	1	eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
                4	EGYPT	4	y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
                5	ETHIOPIA	0	ven packages wake quickly. regu
                6	FRANCE	3	refully final requests. regular, ironi
                7	GERMANY	3	l platelets. regular accounts x-ray: unusual, regular acco
                8	INDIA	2	ss excuses cajole slyly across the packages. deposits print aroun
                9	INDONESIA	2	 slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
                """));
    }

    @Test
    public void testOrders()
            throws Exception
    {
        String sql = "SELECT * FROM tpch.tiny.orders order by 1 limit 10";
        assertArrowResult(sql, ImmutableList.of("""
                orderkey	custkey	orderstatus	totalprice	orderdate	orderpriority	clerk	shippriority	comment
                1	370	O	172799.49	9497	5-LOW	Clerk#000000951	0	nstructions sleep furiously among\s
                2	781	O	38426.09	9831	1-URGENT	Clerk#000000880	0	 foxes. pending accounts at the pending, silent asymptot
                3	1234	F	205654.3	8687	5-LOW	Clerk#000000955	0	sly final accounts boost. carefully regular ideas cajole carefully. depos
                4	1369	O	56000.91	9414	5-LOW	Clerk#000000124	0	sits. slyly regular warthogs cajole. regular, regular theodolites acro
                5	445	F	105367.67	8976	5-LOW	Clerk#000000925	0	quickly. bold deposits sleep slyly. packages use slyly
                6	557	F	45523.1	8086	4-NOT SPECIFIED	Clerk#000000058	0	ggle. special, final requests are against the furiously specia
                7	392	O	271885.66	9505	2-HIGH	Clerk#000000470	0	ly special requests\s
                32	1301	O	198665.57	9327	2-HIGH	Clerk#000000616	0	ise blithely bold, regular requests. quickly unusual dep
                33	670	F	146567.24	8700	3-MEDIUM	Clerk#000000409	0	uriously. furiously final request
                34	611	O	73315.48	10428	3-MEDIUM	Clerk#000000223	0	ly final packages. fluffily final deposits wake blithely ideas. spe
                """));
    }

    @Test
    public void testSingleDateValue()
            throws Exception
    {
        String sql = "SELECT orderdate FROM tpch.tiny.orders order by 1 limit 1";
        assertArrowResult(sql, ImmutableList.of("""
                orderdate
                8035
                """));
    }

    @Test
    public void testCount()
            throws Exception
    {
        String sql = "SELECT * FROM tpch.tiny.orders";
        List<VectorSchemaRoot> vectorSchemaRoots = getVectorSchemaRoots(sql);
        assertEquals(7500, vectorSchemaRoots.stream().map(VectorSchemaRoot::getRowCount).reduce(0, Integer::sum).intValue());
    }

    private List<VectorSchemaRoot> getVectorSchemaRoots(String sql)
        throws Exception
    {
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

            Iterable<List<Object>> data = results.getData();
            if (data != null){
                datas.add(data);
            }

            uri = results.getNextUri();
        }

        BufferAllocator allocator = new RootAllocator();
        List<org.apache.arrow.vector.VectorSchemaRoot> vectorSchemaRoots = new ArrayList<>();

        List<String> input = datas.stream().map(d -> ((Iterable<List<String>>) d).iterator().next().get(0)).toList();

        List<byte[]> decodedChunks = input.stream().map(s -> Base64.decodeBase64(s.getBytes())).toList();
        for (byte[] chunk : decodedChunks) {
            ByteArrayInputStream out = new ByteArrayInputStream(chunk);
            ArrowStreamReader reader = new ArrowStreamReader(out, allocator);
            vectorSchemaRoots.add(reader.getVectorSchemaRoot());
            reader.loadNextBatch();
        }
        System.out.println(vectorSchemaRoots);
        return vectorSchemaRoots;
    }

    private void assertArrowResult(String sql, List<String> expected)
            throws Exception
    {
        List<VectorSchemaRoot> vectorSchemaRoots = getVectorSchemaRoots(sql);
        assertEquals(expected.size(), vectorSchemaRoots.size());
        forEachPair(vectorSchemaRoots.stream(), expected.stream(), (a, b) -> assertEquals(b, a.contentToTSVString()));
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
