package org.neo4j.graphalgo.api;

import java.util.NoSuchElementException;

public abstract class PrimitiveIntBaseIterator implements PrimitiveIntIterator {
    private boolean hasNextDecided;
    private boolean hasNext;
    private int next;

    public PrimitiveIntBaseIterator() {
    }

    public boolean hasNext() {
        if (!this.hasNextDecided) {
            this.hasNext = this.fetchNext();
            this.hasNextDecided = true;
        }

        return this.hasNext;
    }

    public int next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("No more elements in " + this);
        } else {
            this.hasNextDecided = false;
            return this.next;
        }
    }

    protected abstract boolean fetchNext();

    protected boolean next(int nextItem) {
        this.next = nextItem;
        this.hasNext = true;
        return true;
    }
}
