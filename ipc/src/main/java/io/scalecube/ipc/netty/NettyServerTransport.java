package io.scalecube.ipc.netty;

import static io.scalecube.transport.Addressing.MAX_PORT_NUMBER;
import static io.scalecube.transport.Addressing.MIN_PORT_NUMBER;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.ServerStreamConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.BindException;
import java.net.InetAddress;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class NettyServerTransport {

  private final ServerStreamConfig config;
  private final ServerBootstrap serverBootstrap;

  private Channel serverChannel; // calculated
  private Address serverAddress; // calculated

  // Public constructor
  public NettyServerTransport(ServerStreamConfig config, ServerBootstrap serverBootstrap,
      Consumer<ChannelContext> channelContextConsumer) {
    this.config = config;
    this.serverBootstrap = Optional.ofNullable(serverBootstrap).orElse(ServerBootstrapInstanceHolder.DEFAULT_INSTANCE)
        .childHandler(new NettyServiceChannelInitializer(channelContextConsumer));
  }

  // Private constructor
  private NettyServerTransport(NettyServerTransport other) {
    this.config = other.config;
    this.serverBootstrap = other.serverBootstrap;
  }

  public Address getServerAddress() {
    return serverAddress;
  }

  public CompletableFuture<NettyServerTransport> bind() {
    NettyServerTransport transport = new NettyServerTransport(this);

    // Resolve listen IP address
    InetAddress listenAddress =
        Addressing.getLocalIpAddress(config.getListenAddress(), config.getListenInterface(), config.isPreferIPv6());

    // Start binding
    int finalBindPort = config.getPort() + config.getPortCount();
    return transport.bind0(listenAddress, config.getPort(), finalBindPort);
  }

  public CompletableFuture<NettyServerTransport> unbind() {
    CompletableFuture<NettyServerTransport> result = new CompletableFuture<>();
    serverChannel.close().addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        result.complete(NettyServerTransport.this);
      } else {
        Throwable cause = channelFuture.cause();
        result.completeExceptionally(cause);
      }
    });
    return result;
  }

  private CompletableFuture<NettyServerTransport> bind0(InetAddress listenAddress, int bindPort, int finalBindPort) {
    CompletableFuture<NettyServerTransport> result = new CompletableFuture<>();

    // Perform basic bind port validation
    if (bindPort < MIN_PORT_NUMBER || bindPort > MAX_PORT_NUMBER) {
      result.completeExceptionally(
          new IllegalArgumentException("Invalid port number: " + bindPort));
      return result;
    }
    if (bindPort > finalBindPort) {
      result.completeExceptionally(
          new NoSuchElementException("Could not find an available port from: " + bindPort + " to: " + finalBindPort));
      return result;
    }

    // Get address object and bind
    serverAddress = Address.create(listenAddress.getHostAddress(), bindPort);

    // Start binding
    ChannelFuture bindFuture = serverBootstrap.bind(listenAddress, serverAddress.port());
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        serverChannel = channelFuture.channel();
        result.complete(NettyServerTransport.this);
      } else {
        Throwable cause = channelFuture.cause();
        if (config.isPortAutoIncrement() && isAddressAlreadyInUseException(cause)) {
          bind0(listenAddress, bindPort + 1, finalBindPort).thenAccept(result::complete);
        } else {
          result.completeExceptionally(cause);
        }
      }
    });
    return result;
  }

  private boolean isAddressAlreadyInUseException(Throwable exception) {
    return exception instanceof BindException
        || (exception.getMessage() != null && exception.getMessage().contains("Address already in use"));
  }
}
