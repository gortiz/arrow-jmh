package org.github.gortiz.arrow.vector;

import java.util.BitSet;

public class ArrayIntVector extends AbstractVector<Integer>
        implements IntVector {

    private final BitSet validSet;
    private final int[] data;

    public ArrayIntVector(int[] data) {
        this.data = data;
        this.validSet = new BitSet(data.length);
        validSet.set(0, data.length, true);
    }

    public ArrayIntVector(int[] data, BitSet validSet) {
        this.validSet = validSet;
        this.data = data;
        if (validSet.length() > data.length) {
            throw new IllegalArgumentException("Validity is longer than data");
        }
    }

    @Override
    public boolean isPresent(int pos) {
        if (pos > size()) {
            throw new IndexOutOfBoundsException("Position " + pos + " is out "
                    + "of bound (" + size() + ")");
        }
        return validSet.get(pos);
    }

    @Override
    public int getPrimitive(int pos) throws NullPointerException {
        if (!isPresent(pos)) {
            throw new NullPointerException("Position " + pos + " is null");
        }
        return data[pos];
    }

    @Override
    public Integer get(int pos) {
        if (!isPresent(pos)) {
            return null;
        }
        return data[pos];
    }

    @Override
    public int size() {
        return data.length;
    }
}
