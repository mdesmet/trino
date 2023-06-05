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
package io.trino.server.protocol;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Optional;

public class ArrowTypesMapper
{
    private ArrowTypesMapper() {}

    public static Optional<ArrowType> mapToArrow(Type type)
    {
        return Optional.empty();
    }

    public static Optional<ArrowType> mapToArrow(BigintType bigintType)
    {
        return Optional.of(new ArrowType.Int(64, true));
    }

    public static Optional<ArrowType> mapToArrow(CharType charType)
    {
        return Optional.of(ArrowType.Utf8.INSTANCE);
    }

    public static Optional<ArrowType> mapToArrow(BooleanType booleanType)
    {
        return Optional.of(ArrowType.Bool.INSTANCE);
    }

    public static Optional<ArrowType> mapToArrow(DecimalType decimalType)
    {
        return mapToArrow((Type) decimalType); //     return Optional.of(ARROW_BIGINT_TYPE);
    }

    public static Optional<ArrowType> mapToArrow(DoubleType doubleType)
    {
        return Optional.of(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    }

    public static Optional<ArrowType> mapToArrow(IntegerType integerType)
    {
        return Optional.of(new ArrowType.Int(32, true));
    }

    public static Optional<ArrowType> mapToArrow(VarcharType integerType)
    {
        return Optional.of(ArrowType.Utf8.INSTANCE);
    }

    public static Optional<ArrowType> mapToArrow(DateType dateType)
    {
        return Optional.of(new ArrowType.Date(DateUnit.DAY));
    }
}
