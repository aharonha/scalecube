package io.scalecube.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIOServer;
import io.scalecube.ipc.ServerStream;
import io.scalecube.ipc.netty.NettyBootstrapFactory;

public final class GatewayRunner {

  public static void main(String[] args) throws Exception {
    NettyBootstrapFactory.createNew().configureInstance();

    ServerStream serverStream = new ServerStream();
    serverStream.listenReadSuccess().subscribe(event -> serverStream.send(event.getMessage().get()));

    GatewaySocketIOServer.onPort(4040, serverStream::subscribe).start();
    GatewayHttpServer.onPort(8080, serverStream::subscribe).start();

    Thread.currentThread().join();
  }
}
