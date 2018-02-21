package io.scalecube.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIOServer;
import io.scalecube.ipc.ClientStream;
import io.scalecube.ipc.MessageStream;
import io.scalecube.ipc.ServerStream;
import io.scalecube.ipc.netty.NettyBootstrapFactory;
import io.scalecube.transport.Address;

public final class GatewayRunner {

  public static void main(String[] args) throws Exception {
    NettyBootstrapFactory.createNew().configureInstance();

    ClientStream clientStream = MessageStream.newClientStream();
    ServerStream serverStream = MessageStream.newServerStream();

    serverStream.listenReadSuccess().subscribe(event -> {
      System.out.println("Sending ...");
      clientStream.send(Address.create("127.0.1.1", 5801), event.getMessage().get());
    });

    clientStream.listenReadSuccess().subscribe(event -> {
      System.out.println("Got reply: " + event + " sending it back to gateway client");
      serverStream.send(event.getMessage().get());
    });

    GatewaySocketIOServer.onPort(4040, serverStream::subscribe).start();
    GatewayHttpServer.onPort(8080, serverStream::subscribe).start();

    Thread.currentThread().join();
  }
}
