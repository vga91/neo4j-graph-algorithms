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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentralitySuccessorBrandes;
import org.neo4j.graphalgo.impl.betweenness.ParallelBetweennessCentrality;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.rebind;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.withEmptyTx;


/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class BetweennessCentralityIntegrationTest {

    public static final String TYPE = "TYPE";

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();
    
    private static Graph graph;
    private static DefaultBuilder builder;
    private static long centerNodeId;

    @Mock
    private BetweennessCentrality.ResultConsumer consumer;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        builder = GraphBuilder.create(db)
                .setLabel("Node")
                .setRelationship(TYPE);

        final RelationshipType type = RelationshipType.withName(TYPE);

        /**
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         */
        final Node center = builder.newDefaultBuilder()
                .setLabel("Node")
                .createNode();

        centerNodeId = center.getId();

        builder.newRingBuilder()
                .createRing(5)
                .forEachNodeInTx((node, tx) -> {
                    Node nodeBound = rebind(tx, node);
                    Node centerBound = rebind(tx, center);
                    nodeBound.createRelationshipTo(centerBound, type);
                })
                .newRingBuilder()
                .createRing(5)
                .forEachNodeInTx((node, tx) -> {
                    Node nodeBound = rebind(tx, node);
                    Node centerBound = rebind(tx, center);
                    centerBound.createRelationshipTo(nodeBound, type);
                });

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);

        db.getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerProcedure(BetweennessCentralityProc.class);
    }

    @Before
    public void setupMocks() {
        when(consumer.consume(anyLong(), anyDouble()))
                .thenReturn(true);
    }

    @AfterClass
    public static void tearDown() throws Exception {
//        if (db != null) db.shutdown();
        graph = null;
    }

    @Test
    public void testBCDirect() throws Exception {
        new BetweennessCentrality(graph)
                .compute()
                .forEach(consumer);
        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

    @Test
    public void testSuccessorBCDirect() throws Exception {
        new BetweennessCentralitySuccessorBrandes(graph, Pools.DEFAULT)
                .compute()
                .forEach(consumer);
        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

    @Test
    public void testParallelBCDirect() throws Exception {
        new ParallelBetweennessCentrality(graph, Pools.DEFAULT, 4)
                .compute()
                .resultStream()
                .forEach(r -> consumer.consume(r.nodeId, r.centrality));

        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

    @Test
    public void testBetweennessStream() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness.stream('Node', 'TYPE') YIELD nodeId, centrality", row -> {
                    consumer.consume(
                            (long) row.getNumber("nodeId"),
                            (double) row.getNumber("centrality"));
                    return true;
                });

        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

    @Test
    public void testParallelBetweennessStream() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness.stream('Node', 'TYPE', {concurrency:4}) YIELD nodeId, centrality", row -> {
                    consumer.consume(
                            row.getNumber("nodeId").intValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

    @Test
    public void testParallelBetweennessWrite() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness('','', {concurrency:4, write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testParallelBetweennessWriteWithDirection() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness('','', {direction:'<>', concurrency:4, write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertEquals(35.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(30.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(0.5, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testBetweennessWrite() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness('','', {write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testBetweennessWriteWithDirection() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness('','', {direction:'both', write:true, stats:true, writeProperty:'centrality'}) " +
                "YIELD nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertEquals(35.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(30.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(0.5, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandesHighProbability() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness.sampled('','', {strategy:'random', probability:1.0, write:true, " +
                "stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.1);
                    assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.1);
                    assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.1);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandesNoProbability() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness.sampled('','', {strategy:'random', write:true, stats:true, " +
                "writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandeseWrite() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness.sampled('','', {strategy:'random', probability:1.0, " +
                "write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandesStream() throws Exception {

        executeAndAccept(db, "CALL algo.betweenness.sampled.stream('','', {strategy:'random', probability:1.0, " +
                "write:true, stats:true, writeProperty:'centrality'}) YIELD nodeId, centrality", row -> {
                    consumer.consume(
                            row.getNumber("nodeId").intValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }


}
