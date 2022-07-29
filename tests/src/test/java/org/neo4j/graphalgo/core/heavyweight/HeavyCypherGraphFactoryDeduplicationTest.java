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
package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;

public class HeavyCypherGraphFactoryDeduplicationTest {

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    private static int id1;
    private static int id2;

    @BeforeClass
    public static void setUp() {

        executeAndAccept(db, "MERGE (n1 {id: 1}) " + "" +
                "MERGE (n2 {id: 2}) " +
                "CREATE (n1)-[:REL {weight: 4}]->(n2) " +
                "CREATE (n2)-[:REL {weight: 10}]->(n1) " +
                "RETURN id(n1) AS id1, id(n2) AS id2", row -> {
            id1 = row.getNumber("id1").intValue();
            id2 = row.getNumber("id2").intValue();
            return true;
        });
    }


    @Test
    public void testLoadCypher() throws Exception {
        String nodes = "MATCH (n) RETURN id(n) as id";
        String rels = "MATCH (n)-[r]-(m) RETURN id(n) as source, id(m) as target, r.weight as weight";

        final HeavyGraph graph = (HeavyGraph) new TransactionWrapper(db).apply(ktx -> new GraphLoader((GraphDatabaseAPI) db, ktx)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withDuplicateRelationshipsStrategy(DuplicateRelationshipsStrategy.SKIP)
                .load(HeavyCypherGraphFactory.class));

        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.degree(graph.toMappedNodeId(id1), Direction.OUTGOING));
        assertEquals(1, graph.degree(graph.toMappedNodeId(id2), Direction.OUTGOING));
    }
}
