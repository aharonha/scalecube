package io.scalecube.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIOServer;
import io.scalecube.ipc.MessageStream;
import io.scalecube.ipc.ServerStream;

public final class GatewayEchoRunner {

  public static void main(String[] args) throws Exception {
    ServerStream serverStream = MessageStream.newServerStream();
    serverStream.listenReadSuccess().subscribe(event -> serverStream.send(event.getMessage().get()));

    GatewaySocketIOServer.onPort(4040, serverStream::subscribe).start();
    GatewayHttpServer.onPort(8080, serverStream::subscribe).start();

    Thread.currentThread().join();
  }
}
