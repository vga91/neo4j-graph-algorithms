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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class PersonalizedPageRankTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{HeavyCypherGraphFactory.class, "HeavyCypherGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }
    private static final String DB_CYPHER = "" +
            "CREATE (iphone:Product {name:\"iPhone5\"})\n" +
            "CREATE (kindle:Product {name:\"Kindle Fire\"})\n" +
            "CREATE (fitbit:Product {name:\"Fitbit Flex Wireless\"})\n" +
            "CREATE (potter:Product {name:\"Harry Potter\"})\n" +
            "CREATE (hobbit:Product {name:\"Hobbit\"})\n" +

            "CREATE (todd:Person {name:\"Todd\"})\n" +
            "CREATE (mary:Person {name:\"Mary\"})\n" +
            "CREATE (jill:Person {name:\"Jill\"})\n" +
            "CREATE (john:Person {name:\"John\"})\n" +

            "CREATE\n" +
            "  (john)-[:PURCHASED]->(iphone),\n" +
            "  (john)-[:PURCHASED]->(kindle),\n" +
            "  (mary)-[:PURCHASED]->(iphone),\n" +
            "  (mary)-[:PURCHASED]->(kindle),\n" +
            "  (mary)-[:PURCHASED]->(fitbit),\n" +
            "  (jill)-[:PURCHASED]->(iphone),\n" +
            "  (jill)-[:PURCHASED]->(kindle),\n" +
            "  (jill)-[:PURCHASED]->(fitbit),\n" +
            "  (todd)-[:PURCHASED]->(fitbit),\n" +
            "  (todd)-[:PURCHASED]->(potter),\n" +
            "  (todd)-[:PURCHASED]->(hobbit)";

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        db.executeTransactionally(DB_CYPHER);
    }
    public PersonalizedPageRankTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test() throws Exception {
        Label personLabel = Label.label("Person");
        Label productLabel = Label.label("Product");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {

            expected.put(tx.findNode(personLabel, "name", "John").getId(), 0.24851499999999993);
            expected.put(tx.findNode(personLabel, "name", "Jill").getId(), 0.12135449999999998);
            expected.put(tx.findNode(personLabel, "name", "Mary").getId(), 0.12135449999999998);
            expected.put(tx.findNode(personLabel, "name", "Todd").getId(), 0.043511499999999995);

            expected.put(tx.findNode(productLabel, "name", "Kindle Fire").getId(), 0.17415649999999996);
            expected.put(tx.findNode(productLabel, "name", "iPhone5").getId(), 0.17415649999999996);
            expected.put(tx.findNode(productLabel, "name", "Fitbit Flex Wireless").getId(), 0.08085200000000001);
            expected.put(tx.findNode(productLabel, "name", "Harry Potter").getId(), 0.01224);
            expected.put(tx.findNode(productLabel, "name", "Hobbit").getId(), 0.01224);
            tx.commit();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new TransactionWrapper(db).apply(ktx -> new GraphLoader(db, ktx)
                    .withLabel("MATCH (n) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n)-[:PURCHASED]-(m) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl));

        } else {
            graph = new TransactionWrapper(db).apply(ktx -> new GraphLoader(db, ktx)
                    .withDirection(Direction.BOTH)
                    .withRelationshipType("PURCHASED")
                    .asUndirected(true)
                    .load(graphImpl));
        }

        LongStream sourceNodeIds;
        try(Transaction tx = db.beginTx()) {
            Node node = tx.findNode(personLabel, "name", "John");
            sourceNodeIds = LongStream.of(node.getId());
        }

        final CentralityResult rankResult = PageRankAlgorithm
                .of(graph,0.85, sourceNodeIds, Pools.DEFAULT, 2, 1)
                .compute(40)
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });

    }


}
