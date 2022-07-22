package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.function.Consumer;
import java.util.function.Function;

public class TransactionUtil {

    public static  <T> T withTx(GraphDatabaseService db, Function<Transaction, T> action) {
        try (Transaction tx = db.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    // 
    // 
    // todo - usare questa dove serve
    // 
    //
    public static void withEmptyTx(GraphDatabaseService db, Consumer<Transaction> consumer) {
        try (Transaction tx = db.beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }
}
