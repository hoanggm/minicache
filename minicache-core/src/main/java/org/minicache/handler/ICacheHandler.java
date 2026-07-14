package org.minicache.handler;

import org.minicache.common.Message;

public interface ICacheHandler<R> {
    R handle(Message input);
}
