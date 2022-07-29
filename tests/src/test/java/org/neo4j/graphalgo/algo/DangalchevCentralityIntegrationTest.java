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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.DangalchevCentralityProc;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.rebind;


/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class DangalchevCentralityIntegrationTest {

    public static final String TYPE = "TYPE";

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();
    private static DefaultBuilder builder;
    private static long centerNodeId;

    interface TestConsumer {

        void accept(long nodeId, double centrality);
    }

    @Mock
    private TestConsumer consumer;

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

        db.getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerProcedure(DangalchevCentralityProc.class);
    }

    @Test
    public void testClosenessStream() throws Exception {

        executeAndAccept(db, "CALL algo.closeness.dangalchev.stream('Node', 'TYPE') YIELD nodeId, centrality",
                row -> {
                    consumer.accept(
                            row.getNumber("nodeId").longValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verifyMock();
    }

    @Test
    public void testClosenessWrite() throws Exception {

        executeAndAccept(db, "CALL algo.closeness.dangalchev('','', {write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, loadMillis, computeMillis, writeMillis", row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });

        executeAndAccept(db, "MATCH (n) WHERE exists(n.centrality) RETURN id(n) as id, n.centrality as centrality",
                row -> {
                    consumer.accept(
                            row.getNumber("id").longValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verifyMock();
    }

    private void verifyMock() {
        verify(consumer, times(1)).accept(eq(centerNodeId), eq(5.0));
        verify(consumer, times(10)).accept(anyLong(), AdditionalMatchers.eq(3.25, 0.1));
    }
}
