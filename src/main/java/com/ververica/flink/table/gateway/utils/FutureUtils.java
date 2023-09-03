package com.ververica.flink.table.gateway.utils;

import java.util.concurrent.CompletableFuture;

/** A collection of utilities that expand the usage of {@link CompletableFuture}. */
public class FutureUtils {

    // ------------------------------------------------------------------------
    //  Helper methods
    // ------------------------------------------------------------------------

    /**
     * Returns an exceptionally completed {@link CompletableFuture}.
     *
     * @param cause to complete the future with
     * @param <T> type of the future
     * @return An exceptionally completed CompletableFuture
     */
    public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(cause);

        return result;
    }
}
