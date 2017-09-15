package org.github.gortiz.arrow.vector;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class BufferIntVector extends AbstractVector<Integer>
    implements IntVector {
    private final BufferBitVector validity;
    private final IntBuffer dataBuf;

    public BufferIntVector(ByteBuffer validityBuf, IntBuffer dataBuf) {
        this.validity = new BufferBitVector(validityBuf);
        this.dataBuf = dataBuf;
        if (validityBuf.capacity() * 8 > dataBuf.capacity()) {
            throw new IllegalArgumentException("Validity is longer than data (" 
                    + (validityBuf.capacity() * 8) + " vs " 
                    + dataBuf.capacity() + ")");
        }
    }

    @Override
    public boolean isPresent(int pos) {
        return validity.getPrimitive(pos);
    }

    @Override
    public int getPrimitive(int pos) throws NullPointerException {
        if (!isPresent(pos)) {
            throw new NullPointerException("Position " + pos + " is null");
        }
        return dataBuf.get(pos);
    }

    @Override
    public Integer get(int pos) {
        if (!isPresent(pos)) {
            return null;
        }
        return dataBuf.get(pos);
    }

    @Override
    public int size() {
        return dataBuf.capacity() / 4;
    }
}
