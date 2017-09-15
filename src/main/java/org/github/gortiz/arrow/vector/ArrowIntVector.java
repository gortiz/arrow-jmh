package org.github.gortiz.arrow.vector;

import org.apache.arrow.vector.NullableIntVector;


public class ArrowIntVector extends AbstractVector<Integer>
        implements IntVector {

    private final NullableIntVector.Accessor accessor;

    public ArrowIntVector(NullableIntVector.Accessor accessor) {
        this.accessor = accessor;
    }

    @Override
    public boolean isPresent(int pos) {
        return accessor.isSet(pos) != 0;
    }

    public int getPrimitive(int pos) throws NullPointerException {
        return accessor.get(pos);
    }

    @Override
    public Integer get(int pos) {
        return accessor.getObject(pos);
    }

    @Override
    public int size() {
        return accessor.getValueCount();
    }
}
