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
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.function.DoubleConsumer;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.testResult;

/**
 * @author mknblch
 */
public class ShortestPathTest_152 {

    @ClassRule
    public static final DatabaseRule DB = new ImpermanentDatabaseRule();

    private static long startNodeId;
    private static long endNodeId;

    @BeforeClass
    public static void setupGraph() throws KernelException {
        String cypher = "CREATE (a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(e:Loc{name:'E'}), " +
                "(f:Loc{name:'F'}),\n" +
                " (a)-[:ROAD {d:50}]->(b),\n" +
                " (a)-[:ROAD {d:50}]->(c),\n" +
                " (a)-[:ROAD {d:100}]->(d),\n" +
                " (a)-[:RAIL {d:50}]->(d),\n" +
                " (b)-[:ROAD {d:40}]->(d),\n" +
                " (c)-[:ROAD {d:40}]->(d),\n" +
                " (c)-[:ROAD {d:80}]->(e),\n" +
                " (d)-[:ROAD {d:30}]->(e),\n" +
                " (d)-[:ROAD {d:80}]->(f),\n" +
                " (e)-[:ROAD {d:40}]->(f),\n" +
                " (e)-[:RAIL {d:20}]->(f);";

        DB.resolveDependency(GlobalProcedures.class).registerProcedure(ShortestPathProc.class);
        DB.executeTransactionally(cypher);

        try (Transaction tx = DB.beginTx()) {

            startNodeId = tx.findNode(Label.label("Loc"), "name", "A").getId();
            endNodeId = tx.findNode(Label.label("Loc"), "name", "F").getId();

            tx.commit();
        }
    }

    @Test
    public void testDirect() {

        DoubleConsumer mock = mock(DoubleConsumer.class);

        final Graph graph = new GraphLoader(DB, Pools.DEFAULT)
                .withOptionalLabel("Loc")
                .withRelationshipType("ROAD")
                .withOptionalRelationshipWeightsFromProperty("d", 0)
                .load(HeavyGraphFactory.class);

        new ShortestPathDijkstra(graph)
                .compute(startNodeId, endNodeId)
                .resultStream()
                .forEach(r -> mock.accept(r.cost));

        verify(mock, times(1)).accept(eq(0.0));
        verify(mock, times(1)).accept(eq(50.0));
        verify(mock, times(1)).accept(eq(90.0));
        verify(mock, times(1)).accept(eq(120.0));
        verify(mock, times(1)).accept(eq(160.0));
    }

    @Test
    public void testDirectRoadOrRail() {

        DoubleConsumer mock = mock(DoubleConsumer.class);

        final Graph graph = new GraphLoader(DB, Pools.DEFAULT)
                .withOptionalLabel("Loc")
                .withAnyRelationshipType()
                .withOptionalRelationshipWeightsFromProperty("d", 0)
                .load(HeavyGraphFactory.class);

        new ShortestPathDijkstra(graph)
                .compute(startNodeId, endNodeId)
                .resultStream()
                .forEach(r -> mock.accept(r.cost));

        // TODO: this is actually wrong, order should be 0/50/80/100
        // see https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/722 and fix test afterwards
        verify(mock, times(1)).accept(eq(0.0));
        verify(mock, times(1)).accept(eq(50.0));
        verify(mock, times(1)).accept(eq(90.0));
        verify(mock, times(1)).accept(eq(120.0));
        verify(mock, times(1)).accept(eq(140.0));
    }

    @Test
    public void testDijkstraProcedure() {

        DoubleConsumer mock = mock(DoubleConsumer.class);

        String cypher = "MATCH (from:Loc{name:'A'}), (to:Loc{name:'F'}) " +
                "CALL algo.shortestPath.stream(from, to, 'd', {relationshipQuery:'ROAD', defaultValue:999999.0}) " +
                "YIELD nodeId, cost with nodeId, cost MATCH(n) WHERE id(n) = nodeId RETURN n.name as name, cost;";

        testResult(DB, cypher, r -> {
            r.forEachRemaining(rr -> {
                
                System.out.println("uno " + rr.get("name") + ":" + rr.get("cost"));
//                mock.accept(row.getNumber("cost").doubleValue());
//                return true;
            });
        });
        
        executeAndAccept(DB, cypher, row -> {
            System.out.println("due " + row.get("name") + ":" + row.get("cost"));
            mock.accept(row.getNumber("cost").doubleValue());
            return true;
        });

        verify(mock, times(1)).accept(eq(0.0));
        verify(mock, times(1)).accept(eq(50.0));
        verify(mock, times(1)).accept(eq(90.0));
        verify(mock, times(1)).accept(eq(120.0));
        verify(mock, times(1)).accept(eq(160.0));
    }
}
