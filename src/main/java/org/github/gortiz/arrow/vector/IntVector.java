package org.github.gortiz.arrow.vector;

public interface IntVector extends Vector<Integer> {

    int getPrimitive(int pos) throws NullPointerException;

    @Override
    default Integer get(int pos) {
        return isPresent(pos) ? getPrimitive(pos) : null;
    }
}
