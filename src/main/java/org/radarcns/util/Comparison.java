package org.radarcns.util;

import java.util.function.BiFunction;
import java.util.function.Function;

public interface Comparison<V> extends BiFunction<V, V, Integer> {
    static <V, T extends Comparable<T>> Comparison<V> compare(Function<V, T> property) {
        return (a, b) -> property.apply(a).compareTo(property.apply(b));
    }

    default <T extends Comparable<T>> Comparison<V> then(Function<V, T> property) {
        return (a, b) -> {
            int ret = apply(a, b);
            if (ret != 0) {
                return ret;
            }
            return compare(property).apply(a, b);
        };
    }
}
