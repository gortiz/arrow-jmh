/*
 */
package org.github.gortiz.arrow.vector;

import java.util.BitSet;

public class ArrayIntVectorTest extends IntVectorTest {

    @Override
    protected IntVector getVector(BitSet validity, int[] values) {
        return new ArrayIntVector(values, validity);
    }
    
}
