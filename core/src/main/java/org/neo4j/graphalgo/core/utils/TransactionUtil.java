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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

/**
 * TODO
 */
public class TransactionUtil {

    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db, call, emptyMap(), resultConsumer);
    }
    
    public static void testResult(GraphDatabaseService db, String query, Map<String,Object> params, Consumer<Result> resultConsumer) {
        db.executeTransactionally(query, params, res -> {
            resultConsumer.accept(res);
            res.close();
            return null;
        });
    }

    public static long count(GraphDatabaseService db, String cypher) {
        return count(db, cypher, emptyMap());
    }

    public static long count(GraphDatabaseService db, String cypher, Map<String, Object> params) {
        return db.executeTransactionally(cypher, params, Iterators::count);
    }

//    public static  <T> T withTx(GraphDatabaseService db, Function<Transaction, T> action) {
//        
//    }

//    public static  <T> T withTx(GraphDatabaseService db, Function<Transaction, T> action, Map<String,Object> params, ) {
//        try (Transaction tx = db.beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
//    }
    
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
