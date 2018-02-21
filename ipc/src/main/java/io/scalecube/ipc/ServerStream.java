package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyBootstrapFactory;
import io.scalecube.ipc.netty.NettyServerTransport;
import io.scalecube.ipc.netty.NettyServiceChannelInitializer;

import io.netty.bootstrap.ServerBootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

public final class ServerStream extends MessageStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);

  private volatile NettyServerTransport serverTransport; // optional

  ServerStream() {
    // Hint: this is special case when server stream works on top of existing server channels
    serverTransport = null;
  }

  ServerStream(ServerBootstrap serverBootstrap, ServerStreamConfig config) {
    // Hint: this is server stream which works on self allocated server channel
    ServerBootstrap serverBootstrap1 =
        serverBootstrap.childHandler(new NettyServiceChannelInitializer(this::subscribe));
    NettyServerTransport serverTransport = new NettyServerTransport(serverBootstrap1, config);
    serverTransport.bind().thenAccept(transport1 -> this.serverTransport = transport1);
  }

  public void send(ServiceMessage message) {
    ServerStreamUtil.prepareMessageOnSend(message, (identity, message1) -> {
      ChannelContext channelContext = ChannelContext.getIfExist(identity);
      if (channelContext != null) {
        channelContext.postMessageWrite(message1);
        return;
      }
      LOGGER.warn("Failed to handle message: {}, channel context is null by id: {}", message, identity);
      // Hint: at this point plugin fallback behaviour when msg can't be sent
    }, throwable -> {
      LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable);
      // Hint: at this point plugin fallback behaviour when msg can't be sent
    });
  }

  @Override
  public Observable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess).map(event -> {
      ServiceMessage message = event.getMessage().get();
      ServiceMessage message1 = ServerStreamUtil.prepareMessageOnReceive(message, event.getIdentity());
      return Event.copyFrom(event).message(message1).build();
    });
  }

  @Override
  public void close() {
    if (serverTransport != null) {
      serverTransport.unbind();
    }
  }

  public static void main(String[] args) throws Exception {
    NettyBootstrapFactory.createNew().configureInstance();

    ServerStream serverStream = MessageStream.bindServerStream();
    serverStream.listenReadSuccess().subscribe(event -> {
      System.out.println(event);
      serverStream.send(event.getMessage().get());
    });

    Thread.currentThread().join();
  }
}
