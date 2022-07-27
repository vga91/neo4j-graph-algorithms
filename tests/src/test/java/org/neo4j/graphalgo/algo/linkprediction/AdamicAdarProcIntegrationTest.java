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
package org.neo4j.graphalgo.algo.linkprediction;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.linkprediction.LinkPrediction;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AdamicAdarProcIntegrationTest {
    private static final String SETUP =
            "CREATE (mark:Person {name: 'Mark'})\n" +
            "CREATE (michael:Person {name: 'Michael'})\n" +
            "CREATE (praveena:Person {name: 'Praveena'})\n" +
            "CREATE (ryan:Person {name: 'Ryan'})\n" +
            "CREATE (karin:Person {name: 'Karin'})\n" +
            "CREATE (jennifer:Person {name: 'Jennifer'})\n" +
            "CREATE (elaine:Person {name: 'Elaine'})\n" +

            "MERGE (jennifer)-[:FRIENDS]-(ryan)\n" +
            "MERGE (jennifer)-[:FRIENDS]-(karin)\n" +
            "MERGE (elaine)-[:FRIENDS]-(ryan)\n" +
            "MERGE (elaine)-[:FRIENDS]-(karin)\n" +

            "MERGE (mark)-[:FRIENDS]-(michael)\n" +
            "MERGE (mark)-[:WORKS_WITH]->(michael)\n" +

            "MERGE (praveena)-[:FRIENDS]->(michael)";

    private static final DatabaseRule db = new ImpermanentDatabaseRule()
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("algo.*"));

    @BeforeClass
    public static void setUp() throws Exception {

        ((GraphDatabaseAPI) db).getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerFunction(LinkPrediction.class);

        db.executeTransactionally(SETUP);
    }

//    @AfterClass
//    public static void tearDown() {
//        db.shutdown();
//    }

    @Test
    public void oneNodeInCommon() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Mark'})\n" +
                "MATCH (p2:Person {name: 'Praveena'})\n" +
                "RETURN algo.linkprediction.adamicAdar(p1, p2) AS score, " +
                "       1/log(3) AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    public void oneNodeInCommonExplicit() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Mark'})\n" +
                        "MATCH (p2:Person {name: 'Praveena'})\n" +
                        "RETURN algo.linkprediction.adamicAdar(p1, p2, " +
                        "{relationshipQuery: 'FRIENDS', direction: 'BOTH'}) AS score," +
                        "1/log(2) AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
            tx.commit();
        }
    }

    @Test
    public void twoNodesInCommon() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Elaine'})\n" +
                        "RETURN algo.linkprediction.adamicAdar(p1, p2) AS score, " +
                        "       1/log(2) + 1/log(2) AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
            tx.commit();
        }
    }

    @Test
    public void noNeighbors() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Ryan'})\n" +
                        "RETURN algo.linkprediction.adamicAdar(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
            tx.commit();
        }
    }

    @Test
    public void bothNodesTheSame() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Praveena'})\n" +
                        "MATCH (p2:Person {name: 'Praveena'})\n" +
                        "RETURN algo.linkprediction.adamicAdar(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
            tx.commit();
        }
    }

}
