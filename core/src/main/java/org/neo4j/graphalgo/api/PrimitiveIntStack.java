package org.neo4j.graphalgo.api;

import java.util.Arrays;
import java.util.NoSuchElementException;


// TODO - MI SA CHE VIENE USATO SOLO NEL TEST. FORSE METTERLO IN TEST UTIL???
public class PrimitiveIntStack implements PrimitiveIntIterable {
    private int[] array;
    private int cursor;

    public PrimitiveIntStack() {
        this(16);
    }

    public PrimitiveIntStack(int initialSize) {
        this.cursor = -1;
        this.array = new int[initialSize];
    }

    public boolean isEmpty() {
        return this.cursor == -1;
    }

    public void clear() {
        this.cursor = -1;
    }

    public int size() {
        return this.cursor + 1;
    }

    public void close() {
    }

    public PrimitiveIntIterator iterator() {
        return new PrimitiveIntIterator() {
            int idx;

            public boolean hasNext() {
                return this.idx <= PrimitiveIntStack.this.cursor;
            }

            public int next() {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return PrimitiveIntStack.this.array[this.idx++];
                }
            }
        };
    }

//    public void visitKeys(PrimitiveIntVisitor visitor) {
//        throw new UnsupportedOperationException("Please implement");
//    }

    public void push(int value) {
        this.ensureCapacity();
        this.array[++this.cursor] = value;
    }

    private void ensureCapacity() {
        if (this.cursor == this.array.length - 1) {
            this.array = Arrays.copyOf(this.array, this.array.length << 1);
        }

    }

    public int poll() {
        return this.cursor == -1 ? -1 : this.array[this.cursor--];
    }

    public int peek() {
        return this.cursor == -1 ? -1 : this.array[this.cursor];
    }
}