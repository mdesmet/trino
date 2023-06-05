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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Base64;

public class ArrowWriter
{
    private ArrowWriter() {}

    static byte[] toBytes(VectorSchemaRoot vectorSchemaRoot)
    {
        byte[] byteArray;
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());

            byteArray = out.toByteArray();
            return byteArray;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] toBase64Bytes(VectorSchemaRoot vectorSchemaRoot)
    {
        return Base64.getEncoder().encode(toBytes(vectorSchemaRoot));
    }
}
