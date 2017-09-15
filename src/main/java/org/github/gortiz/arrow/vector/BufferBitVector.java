package org.github.gortiz.arrow.vector;

import java.nio.ByteBuffer;

public class BufferBitVector extends AbstractVector<Boolean> {
    private final ByteBuffer data;

    public BufferBitVector(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public boolean isPresent(int pos) {
        return true;
    }

    public boolean getPrimitive(int pos) {
        int byteIndex = pos >> 3;
        byte b = data.get(byteIndex);
        int bitIndex = pos & 7;
        return Long.bitCount(b & (1L << bitIndex)) != 0;
    }

    @Override
    public Boolean get(int pos) {
        return get(pos);
    }

    @Override
    public int size() {
        return data.capacity() << 3;
    }
}
