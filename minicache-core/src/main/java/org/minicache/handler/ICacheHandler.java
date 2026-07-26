package org.minicache.handler;

import org.minicache.common.Message;

import java.util.concurrent.CompletableFuture;

public interface ICacheHandler<R> {
    R handle(Message input);

    CompletableFuture<R> handleAsync(Message input);
}
