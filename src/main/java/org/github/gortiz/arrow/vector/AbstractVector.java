package org.github.gortiz.arrow.vector;

abstract class AbstractVector<E> implements Vector<E> {

    final void checkRange(int from, int to) {
        if (0 > from || from > size()) {
            throw new IndexOutOfBoundsException("Lower bound must be equal " +
                    "or greater than 0 and lower or equal to the size ("
                    + size() + ") but " + from + " was found");
        }
        if (from > to || to > size()) {
            throw new IndexOutOfBoundsException("Upper bound must be equal " +
                    "or greater than lower bound (" + from + ") and lower or " +
                    "equal to the size (" + size() + ") but " + to
                    + " was found");
        }
    }
}
