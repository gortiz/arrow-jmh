package org.github.gortiz.arrow.vector;

import javax.annotation.Nullable;

public interface Vector<E> {


    boolean isPresent(int pos);
    @Nullable
    E get(int pos);

    int size();

}
