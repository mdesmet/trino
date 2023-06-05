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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ArrowReader
{
    public void read(File file)
    {
        try (
                BufferAllocator rootAllocator = new RootAllocator();
                FileInputStream fileInputStream = new FileInputStream(file);
                ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)) {
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
