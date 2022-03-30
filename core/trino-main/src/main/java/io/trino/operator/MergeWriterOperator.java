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
package io.trino.operator;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.block.PageDescriptionUtils;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.split.PageSinkManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class MergeWriterOperator
        extends AbstractRowChangeOperator
{
    private static final Logger log = Logger.get(MergeWriterOperator.class);

    public static class MergeWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final MergeTarget target;
        private final Session session;
        private boolean closed;

        public MergeWriterOperatorFactory(int operatorId, PlanNodeId planNodeId, PageSinkManager pageSinkManager, MergeTarget target, Session session)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            this.target = requireNonNull(target, "target is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, MergeWriterOperator.class.getSimpleName());
            ConnectorMergeSink mergeSink = pageSinkManager.createMergeSink(session, target.getMergeHandle().get());
            return new MergeWriterOperator(context, mergeSink);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MergeWriterOperatorFactory(operatorId, planNodeId, pageSinkManager, target, session);
        }
    }

    private final ConnectorMergeSink mergeSink;

    public MergeWriterOperator(OperatorContext operatorContext, ConnectorMergeSink mergeSink)
    {
        super(operatorContext);
        this.mergeSink = requireNonNull(mergeSink, "mergeSink is null");
    }

    @Override
    public void addInput(Page page)
    {
        log.info("In addInput, page %s", PageDescriptionUtils.describePage(page));
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        mergeSink.storeMergedRows(page);
        rowCount += page.getPositionCount();
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = toListenableFuture(mergeSink.finish());
        }
    }
}
