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

import com.google.common.collect.ImmutableList;
import com.nimbusds.jose.util.Base64;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.client.Column;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class ArrowQueryResultRows
        implements QueryResultRows
{
    private final Optional<List<ColumnAndType>> columns;
    private final List<VectorSchemaRoot> vectorSchemaRoots;

    private final List<Object> base64EncodedVectors;
    private final long totalRows;

    private ArrowQueryResultRows(Session session, Optional<List<ColumnAndType>> columns, List<VectorSchemaRoot> vectorSchemaRoots, Consumer<Throwable> exceptionConsumer)
    {
        // this.session = session.toConnectorSession();
        this.columns = requireNonNull(columns, "columns is null");
        this.vectorSchemaRoots = ImmutableList.copyOf(vectorSchemaRoots);
        this.base64EncodedVectors = encode(vectorSchemaRoots);
        // this.exceptionConsumer = Optional.ofNullable(exceptionConsumer);
        this.totalRows = countRows(vectorSchemaRoots);
        // this.supportsParametricDateTime = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());

        verify(totalRows == 0 || (totalRows > 0 && columns.isPresent()), "data present without columns and types");
    }

    @Override
    public boolean isEmpty()
    {
        return totalRows == 0;
    }

    @Override
    public Optional<List<Column>> getColumns()
    {
        return columns.map(columns -> columns.stream()
                .map(ColumnAndType::getColumn)
                .collect(toImmutableList()));
    }

    @Override
    public long getTotalRowsCount()
    {
        return totalRows;
    }

    @Override
    public Optional<Long> getUpdateCount()
    {
        // We should have exactly single bigint value as an update count.
        if (totalRows != 1 || columns.isEmpty()) {
            return Optional.empty();
        }

        List<ColumnAndType> columns = this.columns.get();

        if (columns.size() != 1 || !columns.get(0).getType().equals(BIGINT)) {
            return Optional.empty();
        }

        checkState(!vectorSchemaRoots.isEmpty(), "no vectorSchemaRoots available");
        VectorSchemaRoot vectorSchemaRoot = vectorSchemaRoots.get(0);
        BigIntVector vector = (BigIntVector) vectorSchemaRoot.getVector(0);
        if (vector.isNull(0)) {
            return Optional.empty();
        }
        return Optional.of(vector.get(0)).map(Number::longValue);
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return ImmutableList.of(base64EncodedVectors).stream().iterator();
    }

    private static long countRows(List<VectorSchemaRoot> vectorSchemaRoots)
    {
        long rows = 0;
        for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
            rows += vectorSchemaRoot.getRowCount();
        }
        return rows;
    }

    private static List<Object> encode(List<VectorSchemaRoot> vectorSchemaRoots)
    {
        ImmutableList.Builder<Object> base64EncodedVectors = ImmutableList.builder();
        for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
            try (
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, /*DictionaryProvider=*/null, Channels.newChannel(out))) {
                writer.writeBatch();
                base64EncodedVectors.add(Base64.encode(out.toByteArray()));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return base64EncodedVectors.build();
    }

    public static Builder arrowQueryResultRowsBuilder(Session session)
    {
        return new Builder(session);
    }

    public static class Builder
    {
        private final Session session;
        private final ImmutableList.Builder<VectorSchemaRoot> vectorSchemaRoots = ImmutableList.builder();
        private Optional<List<ColumnAndType>> columns = Optional.empty();
        private Consumer<Throwable> exceptionConsumer;

        public Builder(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        public Builder addVectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot)
        {
            vectorSchemaRoots.add(vectorSchemaRoot);
            return this;
        }

        public Builder withColumnsAndTypes(@Nullable List<Column> columns, @Nullable List<Type> types)
        {
            if (columns != null || types != null) {
                this.columns = Optional.of(combine(columns, types));
            }

            return this;
        }

        public Builder withExceptionConsumer(Consumer<Throwable> exceptionConsumer)
        {
            this.exceptionConsumer = exceptionConsumer;
            return this;
        }

        public ArrowQueryResultRows build()
        {
            return new ArrowQueryResultRows(
                    session,
                    columns,
                    vectorSchemaRoots.build(),
                    exceptionConsumer);
        }

        private static List<ColumnAndType> combine(@Nullable List<Column> columns, @Nullable List<Type> types)
        {
            checkArgument(columns != null && types != null, "columns and types must be present at the same time");
            checkArgument(columns.size() == types.size(), "columns and types size mismatch");

            ImmutableList.Builder<ColumnAndType> builder = ImmutableList.builderWithExpectedSize(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                builder.add(new ColumnAndType(i, columns.get(i), types.get(i)));
            }

            return builder.build();
        }
    }

    private static class ColumnAndType
    {
        private final int position;
        private final Column column;
        private final Type type;

        private ColumnAndType(int position, Column column, Type type)
        {
            this.position = position;
            this.column = column;
            this.type = type;
        }

        public Column getColumn()
        {
            return column;
        }

        public Type getType()
        {
            return type;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("column", column)
                    .add("type", type)
                    .add("position", position)
                    .toString();
        }
    }
}
