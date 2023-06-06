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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;

public class VectorSchemRootGenerator
{
    private static Schema creatSchema()
    {
        FieldType idType = FieldType.nullable(new ArrowType.Int(32, true));
        Field id = new Field("id", idType, null);

        FieldType nameType = FieldType.nullable(new ArrowType.Utf8());
        Field name = new Field("name", nameType, null);

        FieldType ageType = FieldType.nullable(new ArrowType.Int(32, true));
        Field age = new Field("age", ageType, null);

        FieldType descType = FieldType.nullable(new ArrowType.LargeUtf8());
        Field desc = new Field("desc", descType, null);

        return new Schema(asList(id, name, age, desc));
    }

    public static String randomDesc()
    {
        return randomString(randomBetween(100, 200));
    }

    public static String randomName()
    {
        return randomString(10);
    }

    public static String randomString(int length)
    {
        String symbols = "abcdefghijklmnopqrstuvwxyz";
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = symbols.charAt(ThreadLocalRandom.current().nextInt(symbols.length()));
        }
        return new String(chars);
    }

    public static int randomBetween(int low, int high)
    {
        return ThreadLocalRandom.current().nextInt(high) + low;
    }

    public static void main(String[] args)
            throws IOException
    {
        VectorSchemRootGenerator gen = new VectorSchemRootGenerator();
        int size = 100;
        VectorSchemaRoot root = gen.generate(size);

        String file = "/tmp/arrow/a" + size;
        Files.write(Paths.get(file), ArrowWriter.toBytes(root));

        ArrowReader reader = new ArrowReader();
        reader.read(new File(file));
    }

    public VectorSchemaRoot generate(int numRows)
    {
        Schema schema = creatSchema();
        //TODO Work in progress
        try (
                BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVector = (IntVector) root.getVector("id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector ageVector = (IntVector) root.getVector("age");
            LargeVarCharVector descVector = (LargeVarCharVector) root.getVector("desc");

            idVector.allocateNew(numRows);
            nameVector.allocateNew(numRows);
            ageVector.allocateNew(numRows);
            descVector.allocateNew(numRows);

            for (int i = 0; i < numRows; i++) {
                idVector.set(i, i + 1);
            }
            for (int i = 0; i < numRows; i++) {
                nameVector.set(i, randomName().getBytes(StandardCharsets.UTF_8));
            }
            for (int i = 0; i < numRows; i++) {
                ageVector.set(i, 77);
            }
            for (int i = 0; i < numRows; i++) {
                descVector.set(i, randomDesc().getBytes(StandardCharsets.UTF_8));
            }

            idVector.setValueCount(numRows);
            nameVector.setValueCount(numRows);
            ageVector.setValueCount(numRows);
            descVector.setValueCount(numRows);

            root.setRowCount(numRows);

            System.out.print("TSVS: " + root.contentToTSVString());

            return root;
        }
    }
}
