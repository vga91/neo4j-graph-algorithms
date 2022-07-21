package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.AvailabilityGuard;

import java.util.function.Supplier;

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

