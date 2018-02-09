package io.scalecube.ipc.netty;

import io.scalecube.utils.CopyingModifier;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ThreadFactory;

public final class NettyBootstrapFactory {

  private static final boolean DEFAULT_ENABLE_EPOLL = true;
  private static final int DEFAULT_BOSS_THREADS = 2;
  private static final int DEFAULT_WORKER_THREADS = 0;
  private static final int DEFAULT_CONNECT_TIMEOUT = 3000;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY = new DefaultThreadFactory("sc-boss-ipc", true);
  private static final DefaultThreadFactory WORKER_THREAD_FACTORY = new DefaultThreadFactory("sc-io-ipc", true);

  private static volatile NettyBootstrapFactory singletonInstance;

  private final Config config;

  private boolean isEpollSupported; // calculated
  private EventLoopGroup bossGroup; // calculated
  private EventLoopGroup workerGroup; // calculated

  private NettyBootstrapFactory(Config config) {
    this.config = config;
  }

  //// Factory

  public static NettyBootstrapFactory createNew() {
    return new NettyBootstrapFactory(new Config());
  }

  public static NettyBootstrapFactory createAndConfigureInstance() {
    return createNew().configureInstance();
  }

  public NettyBootstrapFactory connectTimeout(int connectTimeout) {
    return new NettyBootstrapFactory(config.apply(config1 -> config1.connectTimeout = connectTimeout));
  }

  public NettyBootstrapFactory enableEpoll(boolean enableEpoll) {
    return new NettyBootstrapFactory(config.apply(config1 -> config1.enableEpoll = enableEpoll));
  }

  public NettyBootstrapFactory bossThreads(int bossThreads) {
    return new NettyBootstrapFactory(config.apply(config1 -> config1.bossThreads = bossThreads));
  }

  public NettyBootstrapFactory workerThreads(int workerThreads) {
    return new NettyBootstrapFactory(config.apply(config1 -> config1.workerThreads = workerThreads));
  }

  public synchronized NettyBootstrapFactory configureInstance() {
    if (singletonInstance == null) {
      NettyBootstrapFactory factory = new NettyBootstrapFactory(config);
      factory.isEpollSupported = config.enableEpoll && Epoll.isAvailable();
      factory.bossGroup = createEventLoopGroup(factory.isEpollSupported, config.bossThreads, BOSS_THREAD_FACTORY);
      factory.workerGroup = createEventLoopGroup(factory.isEpollSupported, config.workerThreads, WORKER_THREAD_FACTORY);
      NettyBootstrapFactory.singletonInstance = factory;
    }
    return singletonInstance;
  }

  public static NettyBootstrapFactory getInstance() {
    if (singletonInstance == null) {
      throw new IllegalStateException("NettyBootstrapFactory: call configureInstance() method first");
    }
    return singletonInstance;
  }

  public static ServerBootstrap serverBootstrap() {
    return getInstance().serverBootstrap0();
  }

  public static Bootstrap clientBootstrap() {
    return getInstance().clientBootstrap0();
  }

  private ServerBootstrap serverBootstrap0() {
    return new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(serverChannelClass())
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true);
  }

  private Bootstrap clientBootstrap0() {
    return new Bootstrap()
        .group(workerGroup)
        .channel(channelClass())
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout);
  }

  private static EventLoopGroup createEventLoopGroup(boolean isEpollSupported, int threadNum, ThreadFactory tf) {
    return isEpollSupported ? new EpollEventLoopGroup(threadNum, tf) : new NioEventLoopGroup(threadNum, tf);
  }

  private Class<? extends ServerSocketChannel> serverChannelClass() {
    return isEpollSupported ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
  }

  private Class<? extends SocketChannel> channelClass() {
    return isEpollSupported ? EpollSocketChannel.class : NioSocketChannel.class;
  }

  public void shutdown() {
    if (singletonInstance == null) {
      throw new IllegalStateException("NettyBootstrapFactory: call configureInstance() method first");
    }
    this.bossGroup.shutdownGracefully();
    this.workerGroup.shutdownGracefully();
  }

  //// Config

  private static class Config implements CopyingModifier<Config> {

    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private boolean enableEpoll = DEFAULT_ENABLE_EPOLL;
    private int bossThreads = DEFAULT_BOSS_THREADS;
    private int workerThreads = DEFAULT_WORKER_THREADS;

    private Config() {}

    private Config(Config other) {
      this.connectTimeout = other.connectTimeout;
      this.enableEpoll = other.enableEpoll;
      this.bossThreads = other.bossThreads;
      this.workerThreads = other.workerThreads;
    }
  }
}
