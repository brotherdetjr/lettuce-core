/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.output;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Trnasformation wrapper for {@link CommandOutput}.
 *
 * @author Denis Karpov
 */
public class TransformedOutput<K, V, T1, T2> extends CommandOutput<K, V, T2> {

    private static final DummyCodec<?, ?> DUMMY_CODEC = new DummyCodec<>();

    private final CommandOutput<K, V, T1> delegate;
    private final Function<T1, T2> transformation;

    @SuppressWarnings("unchecked")
    public TransformedOutput(CommandOutput<K, V, T1> delegate, Function<T1, T2> transformation) {
        super((RedisCodec<K, V>) DUMMY_CODEC, null);
        this.delegate = delegate;
        this.transformation = transformation;
    }

    @Override
    public void set(ByteBuffer bytes) {
        delegate.set(bytes);
    }

    @Override
    public T2 get() {
        return transformation.apply(delegate.get());
    }

    @Override
    public void setSingle(ByteBuffer bytes) {
        delegate.setSingle(bytes);
    }

    @Override
    public void set(long integer) {
        delegate.set(integer);
    }

    @Override
    public void setError(ByteBuffer error) {
        delegate.setError(error);
    }

    @Override
    public void setError(String error) {
        delegate.setError(error);
    }

    @Override
    public boolean hasError() {
        return delegate.hasError();
    }

    @Override
    public String getError() {
        return delegate.getError();
    }

    @Override
    public void complete(int depth) {
        delegate.complete(depth);
    }

    @Override
    protected String decodeAscii(ByteBuffer bytes) {
        return delegate.decodeAscii(bytes);
    }

    @Override
    public void multi(int count) {
        delegate.multi(count);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [output=").append(get());
        sb.append(", originalOutput='").append(delegate.output).append('\'');
        sb.append(", error='").append(delegate.error).append('\'');
        sb.append(']');
        return sb.toString();
    }

    private static class DummyCodec<K, V> implements RedisCodec<K, V> {
        @Override
        public K decodeKey(ByteBuffer bytes) {
            throw new UnsupportedOperationException("Must not be called!");
        }

        @Override
        public V decodeValue(ByteBuffer bytes) {
            throw new UnsupportedOperationException("Must not be called!");
        }

        @Override
        public ByteBuffer encodeKey(K key) {
            throw new UnsupportedOperationException("Must not be called!");
        }

        @Override
        public ByteBuffer encodeValue(V value) {
            throw new UnsupportedOperationException("Must not be called!");
        }
    }
}
