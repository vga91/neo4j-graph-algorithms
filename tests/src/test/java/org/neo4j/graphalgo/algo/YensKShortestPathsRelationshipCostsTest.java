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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.KShortestPathsProc;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;

/**
 * Graph:
 * <p>
 * (0)
 * /  |  \
 * (4)--(5)--(1)
 * \  /  \ /
 * (3)---(2)
 *
 * @author mknblch
 */
public class YensKShortestPathsRelationshipCostsTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:3.0}]->(b),\n" +
                        " (b)-[:TYPE {cost:2.0}]->(c)";

        DB.executeTransactionally(cypher);
        DB.resolveDependency(GlobalProcedures.class).registerProcedure(KShortestPathsProc.class);
    }

    @Test
    public void test() {
        final String cypher =
                "MATCH (c:Node{name:'c'}), (a:Node{name:'a'}) " +
                "CALL algo.kShortestPaths(c, a, 1, 'cost') " +
                "YIELD resultCount RETURN resultCount";

        executeAndAccept(DB, cypher, row -> {
            assertEquals(1, row.getNumber("resultCount").intValue());
            return true;
        });

        final String shortestPathsQuery = "MATCH p=(:Node {name: $one})-[r:PATH_0*]->(:Node {name: $two})\n" +
                "UNWIND relationships(p) AS pair\n" +
                "return sum(pair.weight) AS distance";

        executeAndAccept(DB, cypher, Map.of("one", "c", "two", "a"), row -> {
            assertEquals(5.0, row.getNumber("distance").doubleValue(), 0.01);
            return true;
        });


    }
}
