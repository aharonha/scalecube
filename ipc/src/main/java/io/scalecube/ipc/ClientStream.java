package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyBootstrapFactory;
import io.scalecube.ipc.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class ClientStream extends MessageStream {

  private final NettyClientTransport clientTransport;

  ClientStream(Bootstrap clientBootstrap) {
    this.clientTransport = new NettyClientTransport(clientBootstrap, this::subscribe);
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


  public static void main(String[] args) throws Exception {
    NettyBootstrapFactory.createNew().configureInstance();

    ClientStream clientStream = MessageStream.newClientStream();
    clientStream.listenWriteError().subscribe(System.err::println, System.err::println, System.err::println);
    clientStream.listenWriteSuccess()
        .subscribe(event -> System.out.println(">>> sent: " + event.getMessage().get()),
            System.err::println,
            () -> System.out.println("listenWriteSuccess Completed"));
    clientStream.listenReadSuccess()
        .subscribe(event -> System.out.println("<<< received: " + event.getMessage().get()),
            System.err::println,
            () -> System.out.println("listenReadSuccess Completed"));

    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
      System.out.println("Sending ...");
      try {
        clientStream.send(Address.create("192.168.1.6", 4801), ServiceMessage.withQualifier("hola").build());
      } catch (Exception e) {
        e.printStackTrace(System.err);
      }
    }, 0, 1, TimeUnit.SECONDS);

    Thread.currentThread().join();
  }
}
