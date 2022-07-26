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

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.AvailabilityGuard;

import java.util.function.Supplier;

/**
 * TODO
 */
public class ThreadToStatementContextBridge implements Supplier<Statement> {
    private final ThreadLocal<KernelTransaction> threadToTransactionMap = new ThreadLocal();
    private final AvailabilityGuard availabilityGuard;

    public ThreadToStatementContextBridge(AvailabilityGuard availabilityGuard) {
        this.availabilityGuard = availabilityGuard;
    }

    public boolean hasTransaction() {
        KernelTransaction kernelTransaction = (KernelTransaction)this.threadToTransactionMap.get();
        if (kernelTransaction != null) {
            this.assertInUnterminatedTransaction(kernelTransaction);
            return true;
        } else {
            return false;
        }
    }

    public void bindTransactionToCurrentThread(KernelTransaction transaction) {
        if (this.hasTransaction()) {
            throw new IllegalStateException(Thread.currentThread() + " already has a transaction bound");
        } else {
            this.threadToTransactionMap.set(transaction);
        }
    }

    public void unbindTransactionFromCurrentThread() {
        this.threadToTransactionMap.remove();
    }

    public Statement get() {
        return this.getKernelTransactionBoundToThisThread(true).acquireStatement();
    }

    public void assertInUnterminatedTransaction() {
        this.assertInUnterminatedTransaction((KernelTransaction)this.threadToTransactionMap.get());
    }

    public KernelTransaction getKernelTransactionBoundToThisThread(boolean strict) {
        KernelTransaction transaction = (KernelTransaction)this.threadToTransactionMap.get();
        if (strict) {
            this.assertInUnterminatedTransaction(transaction);
        }

        return transaction;
    }

    private void assertInUnterminatedTransaction(KernelTransaction transaction) {
        if (this.availabilityGuard.isShutdown()) {
            throw new DatabaseShutdownException();
        } else if (transaction == null) {
            throw new ThreadToStatementContextBridge.BridgeNotInTransactionException();
        } else if (transaction.isTerminated()) {
            throw new TransactionTerminatedException((Status)transaction.getReasonIfTerminated().orElse(Status.Transaction.Terminated));
        }
    }

    private static class BridgeNotInTransactionException extends NotInTransactionException implements Status.HasStatus {
        private BridgeNotInTransactionException() {
        }

        public Status status() {
            // TODO - Status.Request.TransactionRequired
            return Status.Transaction.Request.NoThreadsAvailable;
        }
    }
}

