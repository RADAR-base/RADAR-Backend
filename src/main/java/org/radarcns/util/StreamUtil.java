package org.radarcns.util;

import org.apache.kafka.streams.KeyValue;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class StreamUtil {
    private StreamUtil() {
        // utility class
    }

    public static <K, V> Predicate<KeyValue<K, V>> test(BiPredicate<? super K, ? super V> bip) {
        return entry -> bip.test(entry.key, entry.value);
    }

    public static <K, V, R> Function<KeyValue<K, V>, R> apply(
            BiFunction<? super K, ? super V, R> bif) {
        return entry -> bif.apply(entry.key, entry.value);
    }

    public static <K, V> Function<KeyValue<K, V>, K> first() {
        return e -> e.key;
    }

    public static <K, V> Function<KeyValue<K, V>, V> second() {
        return e -> e.value;
    }

    @FunctionalInterface
    public interface StreamSupplier<T> {
        Stream<T> get();

        default StreamSupplier<T> concat(StreamSupplier<? extends T> other) {
            return () -> Stream.concat(get(), other.get());
        }

        static <T> StreamSupplier<T> supply(StreamSupplier<T> supplier) {
            return supplier;
        }
    }
}
