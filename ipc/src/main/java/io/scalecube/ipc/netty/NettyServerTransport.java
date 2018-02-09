package io.scalecube.ipc.netty;

import static io.scalecube.ipc.netty.NettyBootstrapFactory.serverBootstrap;
import static io.scalecube.transport.Addressing.MAX_PORT_NUMBER;
import static io.scalecube.transport.Addressing.MIN_PORT_NUMBER;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.ServerStream;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;
import io.scalecube.utils.CopyingModifier;

import com.google.common.base.Throwables;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.BindException;
import java.net.InetAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class NettyServerTransport {

  public static final String DEFAULT_LISTEN_ADDRESS = null;
  public static final String DEFAULT_LISTEN_INTERFACE = null; // Default listen settings fallback to getLocalHost
  public static final boolean DEFAULT_PREFER_IP6 = false;
  public static final int DEFAULT_PORT = 4801;
  public static final int DEFAULT_PORT_COUNT = 100;
  public static final boolean DEFAULT_PORT_AUTO_INCREMENT = true;

  private final Config config;

  private Channel serverChannel; // calculated
  private Address address; // calculated

  private boolean started = false;

  private NettyServerTransport(Config config) {
    this.config = config;
  }

  //// Factory

  public static NettyServerTransport withConsumer(Consumer<ChannelContext> channelContextConsumer) {
    Config config = new Config();
    config.channelContextConsumer = channelContextConsumer;
    return new NettyServerTransport(config);
  }

  public NettyServerTransport listenAddress(String listenAddress) {
    return new NettyServerTransport(config.apply(config1 -> config1.listenAddress = listenAddress));
  }

  public NettyServerTransport listenInterface(String listenInterface) {
    return new NettyServerTransport(config.apply(config1 -> config1.listenInterface = listenInterface));
  }

  public NettyServerTransport preferIPv6(boolean preferIPv6) {
    return new NettyServerTransport(config.apply(config1 -> config1.preferIPv6 = preferIPv6));
  }

  public NettyServerTransport onPort(int port) {
    return new NettyServerTransport(config.apply(config1 -> config1.port = port));
  }

  public NettyServerTransport portCount(int portCount) {
    return new NettyServerTransport(config.apply(config1 -> config1.portCount = portCount));
  }

  public NettyServerTransport portAutoIncrement(boolean portAutoIncrement) {
    return new NettyServerTransport(config.apply(config1 -> config1.portAutoIncrement = portAutoIncrement));
  }

  //// Boostrap

  public synchronized void start() {
    if (started) {
      throw new IllegalStateException("Failed to start server: already started");
    }

    // Resolve listen IP address
    InetAddress listenAddress =
        Addressing.getLocalIpAddress(config.listenAddress, config.listenInterface, config.preferIPv6);

    ServerBootstrap serverBootstrap = serverBootstrap()
        .childHandler(new NettyServiceChannelInitializer(config.channelContextConsumer));

    CompletableFuture<Address> future =
        bind0(serverBootstrap, listenAddress, config.port, config.port + config.portCount);

    try {
      future.get(); // => await server binds
    } catch (Exception throwable) {
      throw Throwables.propagate(throwable);
    }
  }

  public synchronized void stop() {
    if (!started) {
      throw new IllegalStateException("Failed to stop server: already stopped");
    }
    serverChannel.close().syncUninterruptibly();
    started = false;
  }

  //// Getters

  public Address getAddress() {
    return address;
  }

  public boolean isStarted() {
    return started;
  }

  private CompletableFuture<Address> bind0(ServerBootstrap serverBootstrap, InetAddress listenAddress, int bindPort,
      int finalBindPort) {

    final CompletableFuture<Address> result = new CompletableFuture<>();

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
    address = Address.create(listenAddress.getHostAddress(), bindPort);
    ChannelFuture bindFuture = serverBootstrap.bind(listenAddress, address.port());
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        serverChannel = channelFuture.channel();
        result.complete(address);
      } else {
        Throwable cause = channelFuture.cause();
        if (config.portAutoIncrement && isAddressAlreadyInUseException(cause)) {
          bind0(serverBootstrap, listenAddress, bindPort + 1, finalBindPort).thenAccept(result::complete);
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

  //// Config

  private static class Config implements CopyingModifier<Config> {

    private String listenAddress = DEFAULT_LISTEN_ADDRESS;
    private String listenInterface = DEFAULT_LISTEN_INTERFACE;
    private boolean preferIPv6 = DEFAULT_PREFER_IP6;
    private int port = DEFAULT_PORT;
    private int portCount = DEFAULT_PORT_COUNT;
    private boolean portAutoIncrement = DEFAULT_PORT_AUTO_INCREMENT;
    private Consumer<ChannelContext> channelContextConsumer;

    private Config() {}

    private Config(Config other) {
      this.listenAddress = other.listenAddress;
      this.listenInterface = other.listenInterface;
      this.preferIPv6 = other.preferIPv6;
      this.port = other.port;
      this.portCount = other.portCount;
      this.portAutoIncrement = other.portAutoIncrement;
      this.channelContextConsumer = other.channelContextConsumer;
    }
  }

  public static void main(String[] args) throws Exception {
    NettyBootstrapFactory.createNew().configureInstance();

    ServerStream serverStream = new ServerStream();
    serverStream.listenReadSuccess().subscribe(event -> {
      System.out.println(event);
      serverStream.send(event.getMessage().get());
    });

    NettyServerTransport.withConsumer(serverStream::subscribe).start();

    Thread.currentThread().join();
  }
}
