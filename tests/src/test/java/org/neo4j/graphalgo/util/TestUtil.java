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


    public static void executeAndAccept(GraphDatabaseService db, String cypher, Result.ResultVisitor<RuntimeException> visitor) {
        try (Transaction tx = db.beginTx()) {
            final Result execute = tx.execute(cypher);
            execute.accept(visitor);
            tx.commit();
        }
    }
}
