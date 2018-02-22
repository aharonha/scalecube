package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;

import java.util.concurrent.CompletableFuture;

public final class ClientStream extends MessageStream {

  private final NettyClientTransport clientTransport;

  ClientStream(Bootstrap bootstrap) {
    this.clientTransport = new NettyClientTransport(bootstrap, this::subscribe);
  }

  public void send(Address address, ServiceMessage message) {
    CompletableFuture<ChannelContext> promise = clientTransport.getOrConnect(address);
    promise.whenComplete((channelContext, throwable) -> {
      if (channelContext != null) {
        channelContext.postMessageWrite(message);
      }
    });
  }

  @Override
  public void close() {
    clientTransport.close();
  }
}
