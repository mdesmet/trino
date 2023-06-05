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
package io.trino.server.arrow;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

@Path("/v1/arrow")
public class ArrowResource
{
    private static final String DATASET_PATH = "/path/to/datasets";

    @GET
    @Path("/{hashId}")
    public Response getDataset(@PathParam("hashId") String hashId)
            throws IOException
    {
        String datasetFilePath = DATASET_PATH + File.separator + hashId + ".arrow";

        File datasetFile = new File(datasetFilePath);
        if (!datasetFile.exists()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        java.nio.file.Path filePath = Paths.get(datasetFilePath);
        File file = filePath.toFile();
        if (!file.exists()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok(file, MediaType.APPLICATION_OCTET_STREAM)
                .header("Content-Disposition", "attachment; filename=\"" + datasetFile.getName() + "\"")
                .build();
    }
}
