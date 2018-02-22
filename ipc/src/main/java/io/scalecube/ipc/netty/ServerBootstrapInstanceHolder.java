package io.scalecube.ipc.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public final class ServerBootstrapInstanceHolder {

  private static final int DEFAULT_BOSS_THREADS = 1;
  private static final int DEFAULT_WORKER_THREADS = 0;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY = new DefaultThreadFactory("sc-boss-ipc", true);
  private static final DefaultThreadFactory WORKER_THREAD_FACTORY = new DefaultThreadFactory("sc-io-ipc", true);

  public static final ServerBootstrap DEFAULT_INSTANCE;

  static {
    EventLoopGroup bossGroup = Epoll.isAvailable()
        ? new EpollEventLoopGroup(DEFAULT_BOSS_THREADS, BOSS_THREAD_FACTORY)
        : new NioEventLoopGroup(DEFAULT_BOSS_THREADS, BOSS_THREAD_FACTORY);

    EventLoopGroup workerGroup = Epoll.isAvailable()
        ? new EpollEventLoopGroup(DEFAULT_WORKER_THREADS, WORKER_THREAD_FACTORY)
        : new NioEventLoopGroup(DEFAULT_WORKER_THREADS, WORKER_THREAD_FACTORY);

    Class<? extends ServerSocketChannel> serverChannelClass = Epoll.isAvailable()
        ? EpollServerSocketChannel.class
        : NioServerSocketChannel.class;

    DEFAULT_INSTANCE = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(serverChannelClass)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true);
  }

  private ServerBootstrapInstanceHolder() {
    // Do not instantiate
  }
}
