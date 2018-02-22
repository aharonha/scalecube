package io.scalecube.ipc.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public final class BootstrapInstanceHolder {

  private static final int DEFAULT_WORKER_THREADS = 0;

  private static final DefaultThreadFactory WORKER_THREAD_FACTORY = new DefaultThreadFactory("sc-io-ipc", true);

  public static final Bootstrap DEFAULT_INSTANCE;

  static {
    EventLoopGroup workerGroup = Epoll.isAvailable()
        ? new EpollEventLoopGroup(DEFAULT_WORKER_THREADS, WORKER_THREAD_FACTORY)
        : new NioEventLoopGroup(DEFAULT_WORKER_THREADS, WORKER_THREAD_FACTORY);

    Class<? extends SocketChannel> channelClass = Epoll.isAvailable()
        ? EpollSocketChannel.class
        : NioSocketChannel.class;

    DEFAULT_INSTANCE = new Bootstrap()
        .group(workerGroup)
        .channel(channelClass)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true);


  }

  private BootstrapInstanceHolder() {
    // Do not instantiate
  }
}
