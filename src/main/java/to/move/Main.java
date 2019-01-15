package to.move;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

// TODO remove me
public class Main {
    public static void main(String[] args) throws Exception {
        RedisClient client = RedisClient.create("redis://localhost");
        streamFlux(client.connect(), "mystream", 20, "0")
                .subscribe(msg -> System.out.println(msg.getBody()));

        System.in.read();
    }

    private static Flux<StreamMessage<String, String>> streamFlux(
            StatefulRedisConnection<String, String> connection, String streamName, long batchSize, String startOffset) {
        AtomicReference<Function<String, Flux<StreamMessage<String, String>>>> xreadFactory = new AtomicReference<>();
        //noinspection unchecked
        xreadFactory.set(offset ->
                connection.reactive().xread(
                        XReadArgs.Builder.count(batchSize).block(0),
                        StreamOffset.from(streamName, offset)
                ).collectList().filter(m -> !m.isEmpty()).flux().flatMap(m ->
                        Flux.fromIterable(m).concatWith(xreadFactory.get().apply(m.get(m.size() - 1).getId()))
                )
        );
        return xreadFactory.get().apply(startOffset);
    }
}
