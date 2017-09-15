/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.github.gortiz.arrow.vector;

import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.NullableIntVector;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode({Mode.Throughput})
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class IntVectorBenchmark {

    /**
     * The size of the vector to test.
     * 
     * To make it more readable, the parameter is, in fact, the base 2 log of
     * the final value.
     */
    @Param({"10", "20", "26"})
    int logSize;
    /**
     * How many ints will be read on sum and sequential tests.
     */
    final int toRead = 1 << 5;
    /**
     * A fixed random value to make the test repeatable.
     */
    final Random r = new Random(57182931);
    final RootAllocator alloc = new RootAllocator(1L << 30);
    NullableIntVector arrowVector;

    private ArrayIntVector arrayV;
    private ArrowIntVector arrowV;
    private BufferIntVector bufferV;

    @Setup(Level.Trial)
    public void setUp() {
        int bytesSize = (int) Math.min(1 << 22, 1l << logSize);
        byte[] bytes = new byte[bytesSize];
        int filled = 0;
        final int dataSize = 4 << logSize;

        ByteBuffer buf = ByteBuffer.allocateDirect(dataSize);

        while (filled < dataSize) {
            r.nextBytes(bytes);
            int toWrite = Math.min(bytes.length, dataSize - filled);
            buf.put(bytes, 0, toWrite);
            filled += toWrite;
        }
        buf.clear();

        createArray(buf);
        createArrow(buf);
        createBuffer(buf);
    }

    @TearDown
    public void tearDown() {
        if (arrowVector != null) {
            arrowVector.close();
        }
        alloc.close();
    }

    private void createArray(ByteBuffer buf) {
        IntBuffer intBuf = buf.asIntBuffer();
        int[] intArr = new int[intBuf.capacity()];
        arrayV = new ArrayIntVector(intArr);
    }

    private void createArrow(ByteBuffer buf) {
        arrowVector = new NullableIntVector("arrowVector", alloc);
        NullableIntVector.Mutator mutator = arrowVector.getMutator();

        IntBuffer intBuf = buf.asIntBuffer();
        arrowVector.allocateNew(intBuf.capacity());
        while (intBuf.hasRemaining()) {
            mutator.set(intBuf.position(), intBuf.get());
        }
        mutator.setValueCount(intBuf.capacity());

        arrowV = new ArrowIntVector(arrowVector.getAccessor());
    }

    private void createBuffer(ByteBuffer buf) {
        IntBuffer intBuf = buf.asIntBuffer();
        ByteBuffer bits = ByteBuffer.allocate(intBuf.capacity() / 8);
        while (bits.hasRemaining()) {
            bits.put((byte) -1);
        }
        
        bits.clear();
        bufferV = new BufferIntVector(bits, intBuf);
    }

    @Benchmark
    public long sumArray() {
        int pos = r.nextInt((1 << logSize) - toRead);
        int max = pos + toRead;
        
        long sum = 0;
        for (int i = pos; i < max; i++) {
            sum += arrayV.getPrimitive(pos);
        }
        return sum;
    }

    @Benchmark
    public long sumArrow() {
        int pos = r.nextInt((1 << logSize) - toRead);
        int max = pos + toRead;
        
        long sum = 0;
        for (int i = pos; i < max; i++) {
            sum += arrowV.getPrimitive(pos);
        }
        return sum;
    }

    @Benchmark
    public long sumBuf() {
        int pos = r.nextInt((1 << logSize) - toRead);
        int max = pos + toRead;
        
        long sum = 0;
        for (int i = pos; i < max; i++) {
            sum += bufferV.getPrimitive(pos);
        }
        return sum;
    }

    @Benchmark
    public void sequentialArray(Blackhole bh) {
        int pos = r.nextInt((1 << logSize) - toRead);
        int max = pos + toRead;
        
        for (int i = pos; i < max; i++) {
            bh.consume(arrayV.getPrimitive(pos));
        }
    }

    @Benchmark
    public void sequentialArrow(Blackhole bh) {
        int pos = r.nextInt((1 << logSize) - toRead);
        int max = pos + toRead;
        
        for (int i = pos; i < max; i++) {
            bh.consume(arrowV.getPrimitive(pos));
        }
    }

    @Benchmark
    public void sequentialBuf(Blackhole bh) {
        int pos = r.nextInt((1 << logSize) - toRead);
        int max = pos + toRead;
        
        for (int i = pos; i < max; i++) {
            bh.consume(bufferV.getPrimitive(pos));
        }
    }

    @Benchmark
    public long randomArray() {
        int pos = r.nextInt(1 << logSize);
        
        return arrayV.getPrimitive(pos);
    }

    @Benchmark
    public long randomArrow() {
        int pos = r.nextInt(1 << logSize);
        
        return arrowV.getPrimitive(pos);
    }

    @Benchmark
    public long randomBuf() {
        int pos = r.nextInt(1 << logSize);
        
        return bufferV.getPrimitive(pos);
    }
}
