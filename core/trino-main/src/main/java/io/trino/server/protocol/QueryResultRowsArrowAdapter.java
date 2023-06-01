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

import io.trino.client.Column;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryResultRowsArrowAdapter
        implements QueryResultRows
{
    private final QueryResultRows queryResultRows;
    private final List<List<Object>> arrowUris;

    public QueryResultRowsArrowAdapter(QueryResultRows queryResultRows, List<List<Object>> arrowUris)
    {
        this.queryResultRows = requireNonNull(queryResultRows, "queryResultRows is null");
        this.arrowUris = requireNonNull(arrowUris, "arrowUris is null");
    }

    @Override
    public boolean isEmpty()
    {
        return queryResultRows.isEmpty();
    }

    @Override
    public Optional<List<Column>> getColumns()
    {
        return queryResultRows.getColumns();
    }

    @Override
    public long getTotalRowsCount()
    {
        return queryResultRows.getTotalRowsCount();
    }

    @Override
    public Optional<Long> getUpdateCount()
    {
        return queryResultRows.getUpdateCount();
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return arrowUris.iterator();
    }
}
