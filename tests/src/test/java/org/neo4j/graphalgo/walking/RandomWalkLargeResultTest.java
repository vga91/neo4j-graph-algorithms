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
package org.neo4j.graphalgo.walking;

import org.junit.*;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.*;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.count;

public class RandomWalkLargeResultTest {

    private static final int NODE_COUNT = 20000;

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void beforeClass() throws KernelException {
        db.getDependencyResolver().resolveDependency(GlobalProcedures.class).registerProcedure(NodeWalkerProc.class);

        db.executeTransactionally(buildDatabaseQuery(), Collections.singletonMap("count",NODE_COUNT));
    }

    private static String buildDatabaseQuery() {
        return "UNWIND range(0,$count) as id " +
                "CREATE (n:Node) " +
                "WITH collect(n) as nodes " +
                "unwind nodes as n with n, nodes[toInteger(rand()*10000)] as m " +
                "create (n)-[:FOO]->(m)";
    }

    @Test
    public void shouldHandleLargeResults() {
        final long count = count(db, "CALL algo.randomWalk.stream(null, 100, 100000)");

        assertEquals(100000, count);
    }
}
