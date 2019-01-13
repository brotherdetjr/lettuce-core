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

import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Single {@link StreamMessage} response.
 *
 * @author Denis Karpov
 */
public class StreamMessageOutput<K, V> extends CommandOutput<K, V, StreamMessage<K, V>> {

    private String id;
    private K key;
    private Map<K, V> body;
    private boolean compound;

    public StreamMessageOutput(RedisCodec<K, V> codec, K stream) {
        super(codec, new StreamMessage<>(stream, null, null));
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (id == null) {
            id = decodeAscii(bytes);
            return;
        }

        if (key == null) {
            key = codec.decodeKey(bytes);
            return;
        }

        if (body == null) {
            body = new LinkedHashMap<>();
        }

        body.put(key, bytes == null ? null : codec.decodeValue(bytes));
        key = null;
    }

    @Override
    public void complete(int depth) {
        if (depth == 0) {
            if (compound && body == null) {
                body = emptyMap();
            }
            output = new StreamMessage<>(output.getStream(), id, body);
        } else {
            compound = true;
        }
    }
}
