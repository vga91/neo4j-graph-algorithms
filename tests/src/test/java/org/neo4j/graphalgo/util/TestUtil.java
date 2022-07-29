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
package org.neo4j.graphalgo.util;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TestUtil {
    
//    public static void regi1sterProcedures(GraphDatabaseService db, Class<?>...procedures) {
//        GlobalProcedures globalProcedures = getGlobalProcs((GraphDatabaseAPI) db);
//        for (Class<?> procedure : procedures) {
//            try {
//                globalProcedures.11re1gisterProcedure(procedure);
//            } catch (KernelException e) {
//                throw new RuntimeException("while registering procedure " + procedure, e);
//            }
//        }
//    }
//    
//    public static void regi1sterFunctions(GraphDatabaseService db, Class<?>...functions) {
//        GlobalProcedures globalProcedures = getGlobalProcs((GraphDatabaseAPI) db);
//        for (Class<?> function : functions) {
//            try {
//                globalProcedures.11re1gisterFunction(function);
//            } catch (KernelException e) {
//                throw new RuntimeException("while registering function " + function, e);
//            }
//        }
//    }

    private static GlobalProcedures getGlobalProcs(GraphDatabaseAPI db) {
        return db.getDependencyResolver().resolveDependency(GlobalProcedures.class);
    }
}
