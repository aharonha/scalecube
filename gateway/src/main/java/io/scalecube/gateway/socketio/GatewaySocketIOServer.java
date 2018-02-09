package io.scalecube.gateway.socketio;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.netty.NettyBootstrapFactory;
import io.scalecube.socketio.ServerConfiguration;
import io.scalecube.socketio.SocketIOServer;
import io.scalecube.utils.CopyingModifier;

import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

public final class GatewaySocketIOServer {

  private static final int DEFAULT_MAX_FRAME_LENGTH = 65536;
  private static final int SOCKETIO_DEFAULT_HEARTBEAT_TIMEOUT = 60;
  private static final int SOCKETIO_DEFAULT_HEARTBEAT_INTERVAL = 25;
  private static final int SOCKETIO_DEFAULT_CLOSE_TIMEOUT = 60;
  private static final String SOCKETIO_DEFAULT_TRANSPORTS = "websocket,flashsocket,xhr-polling,jsonp-polling";
  private static final boolean SOCKETIO_DEFAULT_ALWAYS_SECURE_WEB_SOCKET_LOCATION = false;
  private static final String SOCKETIO_DEFAULT_REMOTE_ADDRESS_HEADER = "X-Forwarded-For";
  private static final boolean EVENT_EXECUTOR_ENABLED = false;

  private final Config config;

  private SocketIOServer socketIOServer; // calculated

  private GatewaySocketIOServer(Config config) {
    this.config = config;
  }

  //// Factory

  public static GatewaySocketIOServer onPort(int port, Consumer<ChannelContext> channelContextConsumer) {
    Config config = new Config();
    config.port = port;
    config.channelContextConsumer = channelContextConsumer;
    return new GatewaySocketIOServer(config);
  }

  public GatewaySocketIOServer withSsl(SSLContext sslContext) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.sslContext = sslContext));
  }

  public GatewaySocketIOServer closeTimeout(int closeTimeout) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.closeTimeout = closeTimeout));
  }

  public GatewaySocketIOServer heartbeatInterval(int heartbeatInterval) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.heartbeatInterval = heartbeatInterval));
  }

  public GatewaySocketIOServer heartbeatTimeout(int heartbeatTimeout) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.heartbeatTimeout = heartbeatTimeout));
  }

  public GatewaySocketIOServer maxWebSocketFrameSize(int maxWebSocketFrameSize) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.maxWebSocketFrameSize = maxWebSocketFrameSize));
  }

  public GatewaySocketIOServer alwaysSecureWebSocketLocation(boolean alwaysSecureWebSocketLocation) {
    return new GatewaySocketIOServer(
        config.apply(config1 -> config1.alwaysSecureWebSocketLocation = alwaysSecureWebSocketLocation));
  }

  public GatewaySocketIOServer remoteAddressHeader(String remoteAddressHeader) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.remoteAddressHeader = remoteAddressHeader));
  }

  public GatewaySocketIOServer transports(String transports) {
    return new GatewaySocketIOServer(config.apply(config1 -> config1.transports = transports));
  }

  //// Bootstrap

  public synchronized void start() {
    if (socketIOServer != null && socketIOServer.isStarted()) {
      throw new IllegalStateException("Failed to start server: already started");
    }

    ServerConfiguration configuration = ServerConfiguration.builder()
        .port(config.port)
        .sslContext(config.sslContext)
        .closeTimeout(config.closeTimeout)
        .alwaysSecureWebSocketLocation(config.alwaysSecureWebSocketLocation)
        .heartbeatInterval(config.heartbeatInterval)
        .heartbeatTimeout(config.heartbeatTimeout)
        .maxWebSocketFrameSize(config.maxWebSocketFrameSize)
        .remoteAddressHeader(config.remoteAddressHeader)
        .transports(config.transports)
        .eventExecutorEnabled(EVENT_EXECUTOR_ENABLED)
        .build();

    SocketIOServer server = SocketIOServer.newInstance(configuration);
    server.setListener(new GatewaySocketIOListener(config.channelContextConsumer));
    server.setServerBootstrapFactory(NettyBootstrapFactory::serverBootstrap);
    server.start();

    socketIOServer = server;
  }

  public synchronized void stop() {
    if (socketIOServer != null && socketIOServer.isStopped()) {
      throw new IllegalStateException("Failed to stop server: already stopped");
    }
    if (socketIOServer != null) {
      socketIOServer.stop();
    }
  }

  //// Config

  private static class Config implements CopyingModifier<Config> {

    private SSLContext sslContext;
    private int port;
    private String transports = SOCKETIO_DEFAULT_TRANSPORTS;
    private int heartbeatTimeout = SOCKETIO_DEFAULT_HEARTBEAT_TIMEOUT;
    private int heartbeatInterval = SOCKETIO_DEFAULT_HEARTBEAT_INTERVAL;
    private int closeTimeout = SOCKETIO_DEFAULT_CLOSE_TIMEOUT;
    private int maxWebSocketFrameSize = DEFAULT_MAX_FRAME_LENGTH;
    private boolean alwaysSecureWebSocketLocation = SOCKETIO_DEFAULT_ALWAYS_SECURE_WEB_SOCKET_LOCATION;
    private String remoteAddressHeader = SOCKETIO_DEFAULT_REMOTE_ADDRESS_HEADER;
    private Consumer<ChannelContext> channelContextConsumer;

    private Config() {}

    private Config(Config other) {
      this.sslContext = other.sslContext;
      this.port = other.port;
      this.transports = other.transports;
      this.heartbeatTimeout = other.heartbeatTimeout;
      this.heartbeatInterval = other.heartbeatInterval;
      this.closeTimeout = other.closeTimeout;
      this.maxWebSocketFrameSize = other.maxWebSocketFrameSize;
      this.alwaysSecureWebSocketLocation = other.alwaysSecureWebSocketLocation;
      this.remoteAddressHeader = other.remoteAddressHeader;
      this.channelContextConsumer = other.channelContextConsumer;
    }
  }
}
