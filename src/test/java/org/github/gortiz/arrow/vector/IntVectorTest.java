/*
 */

package org.github.gortiz.arrow.vector;

import java.util.BitSet;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public abstract class IntVectorTest {
    
    protected abstract IntVector getVector(BitSet validity, int[] values);
    
    public void testEquals(BitSet validity, int[] values) {
        IntVector vector = getVector(validity, values);
        
        for (int i = 0; i < values.length; i++) {
            int expected = values[i];
            if (validity.get(i)) {
                Assert.assertTrue("Error on pos " + i, vector.isPresent(i));
                
                int found = vector.getPrimitive(i);

                Assert.assertEquals("Error on pos" + i, expected, found);
            } else {
                Assert.assertFalse("Error on pos " + i, vector.isPresent(i));
                
                Integer found = vector.get(i);
                
                Assert.assertNull("Error on pos" + i, found);
            }
        }
    }
    
    @Test
    public void testAllSet() {
        Random r = new Random();
        int[] values = r.ints(10_000).toArray();
        BitSet validity = new BitSet(values.length);
        validity.set(0, values.length, true);
        
        testEquals(validity, values);
    }

}
