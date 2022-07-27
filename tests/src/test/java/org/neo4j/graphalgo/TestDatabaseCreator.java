///*
// * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
// *
// * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
// *
// * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.graphalgo;
//
//import org.junit.Ignore;
//import org.neo4j.configuration.GraphDatabaseSettings;
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
//import org.neo4j.kernel.internal.GraphDatabaseAPI;
//import org.neo4j.test.TestDatabaseManagementServiceBuilder;
//
//import java.io.File;
//import java.util.UUID;
//
///**
// * @author mh
// * @since 13.10.17
// */
//@Ignore
//public class TestDatabaseCreator {
//
//    public static GraphDatabaseAPI createTestDatabase() {
//        // 
//        // 
//        // TODO - QUESTO PER TUTTI NON ANDREBBE BENE?
//        // 
//        // 
//
//        final DatabaseManagementService databaseManagementService = new TestDatabaseManagementServiceBuilder(new File(UUID.randomUUID().toString()).toPath())
//                .build();
//        return (GraphDatabaseAPI) databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
//    }
//}
