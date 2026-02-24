package org.radarbase.util

fun interface Comparison<V> : (V, V) -> Int {
    companion object {
        fun <V, T : Comparable<T>> compare(property: (V) -> T): Comparison<V> =
            Comparison { a, b -> property(a).compareTo(property(b)) }
    }

    fun <T : Comparable<T>> then(property: (V) -> T): Comparison<V> = Comparison { a, b ->
        val ret = this(a, b)
        if (ret != 0) ret else compare(property)(a, b)
    }
}
