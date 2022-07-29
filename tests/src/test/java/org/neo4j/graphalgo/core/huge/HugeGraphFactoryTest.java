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
package org.neo4j.graphalgo.core.huge;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class HugeGraphFactoryTest {

    @ClassRule
    public static DatabaseRule DB = new ImpermanentDatabaseRule();

    private static long id1;
    private static long id2;
    private static long id3;

    @BeforeClass
    public static void setup() {
        DB.executeTransactionally("CREATE " +
                "(n1:Node1 {prop1: 1})," +
                "(n2:Node2 {prop2: 2})," +
                "(n3:Node3 {prop3: 3})" +
                "CREATE " +
                "(n1)-[:REL1 {prop1: 1}]->(n2)," +
                "(n1)-[:REL2 {prop2: 2}]->(n3)," +
                "(n2)-[:REL3 {prop3: 3}]->(n3);");
        id1 = DB.executeTransactionally("MATCH (n:Node1) RETURN id(n) AS id", Map.of(), r -> r.<Long>columnAs("id").next());
        id2 = DB.executeTransactionally("MATCH (n:Node2) RETURN id(n) AS id", Map.of(), r -> r.<Long>columnAs("id").next());
        id3 = DB.executeTransactionally("MATCH (n:Node3) RETURN id(n) AS id", Map.of(), r -> r.<Long>columnAs("id").next());
    }

    @Test
    public void testAnyLabel() {

        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class));

        assertEquals(3L, graph.nodeCount());
    }

    @Test
    public void testWithLabel() {

        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withLabel("Node1")
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class));

        assertEquals(1L, graph.nodeCount());
    }

    @Test
    public void testAnyRelation() {
        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class));

        long[] out1 = collectTargetIds(graph, id1);
        assertArrayEquals(expectedIds(graph, id2, id3), out1);

        long[] out2 = collectTargetIds(graph, id2);
        assertArrayEquals(expectedIds(graph, id3), out2);
    }

    @Test
    public void testWithRelation() {
        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withRelationshipType("REL1")
                .load(HugeGraphFactory.class));

        long[] out1 = collectTargetIds(graph, id1);
        assertArrayEquals(expectedIds(graph, id2), out1);

        long[] out2 = collectTargetIds(graph, id2);
        assertArrayEquals(expectedIds(graph), out2);
    }

    @Test
    public void testWithProperty() {

        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("prop1", 0.0)
                .load(HugeGraphFactory.class));

        double[] out1 = collectTargetWeights(graph, id1);
        assertArrayEquals(expectedWeights(1.0, 0.0), out1, 1e-4);
    }

    @Test
    public void testWithNodeProperties() {
        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .withOptionalNodeProperties(
                        PropertyMapping.of("prop1", "prop1", 0D),
                        PropertyMapping.of("prop2", "prop2", 0D),
                        PropertyMapping.of("prop3", "prop3", 0D)
                )
                .load(HugeGraphFactory.class));

        assertEquals(1.0, graph.nodeProperties("prop1").get(graph.toMappedNodeId(0L)), 0.01);
        assertEquals(2.0, graph.nodeProperties("prop2").get(graph.toMappedNodeId(1L)), 0.01);
        assertEquals(3.0, graph.nodeProperties("prop3").get(graph.toMappedNodeId(2L)), 0.01);
    }

    @Test
    public void testWithHugeNodeProperties() {
        final HugeGraph graph = (HugeGraph) new TransactionWrapper(DB).apply(ktx -> new GraphLoader(DB, ktx)
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .withOptionalNodeProperties(
                        PropertyMapping.of("prop1", "prop1", 0D),
                        PropertyMapping.of("prop2", "prop2", 0D),
                        PropertyMapping.of("prop3", "prop3", 0D)
                )
                .load(HugeGraphFactory.class));

        assertEquals(1.0, graph.hugeNodeProperties("prop1").nodeWeight(graph.toHugeMappedNodeId(0L)), 0.01);
        assertEquals(2.0, graph.hugeNodeProperties("prop2").nodeWeight(graph.toHugeMappedNodeId(1L)), 0.01);
        assertEquals(3.0, graph.hugeNodeProperties("prop3").nodeWeight(graph.toHugeMappedNodeId(2L)), 0.01);
    }

    private long[] collectTargetIds(final HugeGraph graph, long sourceId) {
        LongStream.Builder outIds = LongStream.builder();
        graph.forEachRelationship(graph.toHugeMappedNodeId(sourceId), Direction.OUTGOING,
                (sourceNodeId, targetNodeId) -> {
                    outIds.add(targetNodeId);
                    return true;
                });
        return outIds.build().sorted().toArray();
    }

    private double[] collectTargetWeights(final HugeGraph graph, long sourceId) {
        DoubleStream.Builder outWeights = DoubleStream.builder();
        graph.forEachRelationship(graph.toHugeMappedNodeId(sourceId), Direction.OUTGOING,
                (sourceNodeId, targetNodeId, weight) -> {
                    outWeights.add(weight);
                    return true;
                });
        return outWeights.build().toArray();
    }

    private long[] expectedIds(final HugeGraph graph, long... expected) {
        return Arrays.stream(expected).map(graph::toHugeMappedNodeId).sorted().toArray();
    }

    private double[] expectedWeights(double... expected) {
        return expected;
    }
}
