/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class StatementAction extends StatementApi implements RenamesCurrentThread, Runnable, StatementApi.TxConsumer {

    protected StatementAction(GraphDatabaseAPI api) {
        super(api);
    }

    @Override
    public void run() {
        try (Revert ignored = RenamesCurrentThread.renameThread(threadName())) {
            acceptInTransaction(this);
        // todo - in 3.5 Exception.throwIfUnchecked block comment says "Do note that if the segment common code is missing, it's preferable to use this instead: catch (RuntimeException | Error e) {...}"
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
