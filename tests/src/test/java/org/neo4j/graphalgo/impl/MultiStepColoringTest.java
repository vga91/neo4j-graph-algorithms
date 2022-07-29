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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.getKernelTx;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.withEmptyTx;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.withTx;

/**
 * @author mknblch
 */
public class MultiStepColoringTest {

    public static final int NUM_SETS = 20;
    public static final int SET_SIZE = 1000;


    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    @ClassRule
    public static final DatabaseRule DB = new ImpermanentDatabaseRule();

    private Graph graph;

    @Before
    public void setup() throws Exception {
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph();
        }

        graph = new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class));
    }

    private static void createTestGraph() throws Exception {
        final int rIdx = withTx(DB, tx -> {
            KernelTransaction transaction = getKernelTx(tx);
            try {
                return transaction.tokenWrite().relationshipTypeGetOrCreateForName(RELATIONSHIP_TYPE.name());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < NUM_SETS; i++) {
            runnables.add(createRing(rIdx));
        }
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    private static Runnable createRing(int rIdx) {
        return () -> {
            withTx(DB, (Transaction tx) -> {
                try {
                    KernelTransaction transaction = getKernelTx(tx);
                    final Write op = transaction.dataWrite();
                    long node = op.nodeCreate();
                    long start = node;
                    for (int i = 1; i < SET_SIZE; i++) {
                        final long temp = op.nodeCreate();
                        op.relationshipCreate(node, rIdx, temp);
                        node = temp;
                    }
                    // todo  - test that
                    return op.relationshipCreate(node, rIdx, start);
                } catch (EntityNotFoundException | InvalidTransactionTypeKernelException e) {
                    throw new RuntimeException(e);
                }
            });
        };
    }

    @Test
    public void testMsColoring() {

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("MSColoring took " + l + "ms"))) {
            final AtomicIntegerArray colors = new MSColoring(graph, Pools.DEFAULT, 8)
                    .compute()
                    .getColors();

            assertEquals(NUM_SETS, numColors(colors));
        }
    }

    private static int numColors(AtomicIntegerArray colors) {
        final IntIntMap map = new IntIntScatterMap();
        for (int i = colors.length() - 1; i >= 0; i--) {
            map.addTo(colors.get(i), 1);
        }
        return map.size();
    }
}
