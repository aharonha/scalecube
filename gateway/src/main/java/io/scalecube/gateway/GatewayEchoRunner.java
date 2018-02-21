package io.scalecube.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIOServer;
import io.scalecube.ipc.ServerStream;
import io.scalecube.ipc.ServiceMessageStream;
import io.scalecube.ipc.netty.NettyBootstrapFactory;

public final class GatewayEchoRunner {

  public static void main(String[] args) throws Exception {
    NettyBootstrapFactory.createNew().configureInstance();

    ServerStream serverStream = ServiceMessageStream.newServerStream();
    serverStream.listenReadSuccess().subscribe(event -> serverStream.send(event.getMessage().get()));

    GatewaySocketIOServer.onPort(4040, serverStream::subscribe).start();
    GatewayHttpServer.onPort(8080, serverStream::subscribe).start();

    Thread.currentThread().join();
  }
}
