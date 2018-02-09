package io.scalecube.gateway.http;

import static io.scalecube.ipc.netty.NettyBootstrapFactory.serverBootstrap;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.utils.CopyingModifier;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

public final class GatewayHttpServer {

  private static final int DEFAULT_MAX_FRAME_LENGTH = 2048000;
  private static final boolean DEFAULT_CORS_ENABLED = false;
  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN = "*";
  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_METHODS = "GET, POST, OPTIONS";
  private static final int DEFAULT_ACCESS_CONTROL_MAX_AGE = 86400;

  private final Config config;

  private Channel serverChannel; // calculated

  private boolean started = false;

  private GatewayHttpServer(Config config) {
    this.config = config;
  }

  //// Factory

  public static GatewayHttpServer onPort(int port, Consumer<ChannelContext> channelContextConsumer) {
    Config config = new Config();
    config.port = port;
    config.channelContextConsumer = channelContextConsumer;
    return new GatewayHttpServer(config);
  }

  public GatewayHttpServer withSsl(SSLContext sslContext) {
    return new GatewayHttpServer(config.apply(config1 -> config1.sslContext = sslContext));
  }

  public GatewayHttpServer maxFrameLength(int maxFrameLength) {
    return new GatewayHttpServer(config.apply(config1 -> config1.maxFrameLength = maxFrameLength));
  }

  public GatewayHttpServer corsEnabled(boolean corsEnabled) {
    return new GatewayHttpServer(config.apply(config1 -> config1.corsEnabled = corsEnabled));
  }

  public GatewayHttpServer accessControlAllowOrigin(String accessControlAllowOrigin) {
    return new GatewayHttpServer(config.apply(config1 -> config1.accessControlAllowOrigin = accessControlAllowOrigin));
  }

  public GatewayHttpServer accessControlAllowMethods(String accessControlAllowMethods) {
    return new GatewayHttpServer(
        config.apply(config1 -> config1.accessControlAllowMethods = accessControlAllowMethods));
  }

  public GatewayHttpServer accessControlMaxAge(int accessControlMaxAge) {
    return new GatewayHttpServer(config.apply(config1 -> config1.accessControlMaxAge = accessControlMaxAge));
  }

  //// Bootstrap

  public synchronized void start() {
    if (started) {
      throw new IllegalStateException("Failed to start server: already started");
    }

    serverChannel = serverBootstrap()
        .childHandler(new GatewayHttpChannelInitializer(config, config.channelContextConsumer))
        .bind(new InetSocketAddress(config.port))
        .syncUninterruptibly()
        .channel();

    started = true;
  }

  public synchronized void stop() {
    if (!started) {
      throw new IllegalStateException("Failed to stop server: already stopped");
    }
    serverChannel.close().syncUninterruptibly();
    started = false;
  }

  //// Config

  static class Config implements CopyingModifier<Config> {

    private SSLContext sslContext;
    private int port;
    private int maxFrameLength = DEFAULT_MAX_FRAME_LENGTH;
    private boolean corsEnabled = DEFAULT_CORS_ENABLED;
    private String accessControlAllowOrigin = DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN;
    private String accessControlAllowMethods = DEFAULT_ACCESS_CONTROL_ALLOW_METHODS;
    private int accessControlMaxAge = DEFAULT_ACCESS_CONTROL_MAX_AGE;
    private Consumer<ChannelContext> channelContextConsumer;

    private Config() {}

    private Config(Config other) {
      this.sslContext = other.sslContext;
      this.port = other.port;
      this.maxFrameLength = other.maxFrameLength;
      this.corsEnabled = other.corsEnabled;
      this.accessControlAllowOrigin = other.accessControlAllowOrigin;
      this.accessControlAllowMethods = other.accessControlAllowMethods;
      this.accessControlMaxAge = other.accessControlMaxAge;
      this.channelContextConsumer = other.channelContextConsumer;
    }

    SSLContext getSslContext() {
      return sslContext;
    }

    int getPort() {
      return port;
    }

    int getMaxFrameLength() {
      return maxFrameLength;
    }

    boolean isCorsEnabled() {
      return corsEnabled;
    }

    String getAccessControlAllowOrigin() {
      return accessControlAllowOrigin;
    }

    String getAccessControlAllowMethods() {
      return accessControlAllowMethods;
    }

    int getAccessControlMaxAge() {
      return accessControlMaxAge;
    }
  }
}
