package io.scalecube.gateway.http;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.netty.ChannelContextHandler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import javax.net.ssl.SSLEngine;

@Sharable
public final class GatewayHttpChannelInitializer extends ChannelInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpChannelInitializer.class);

  private final GatewayHttpServer.Config config;
  private final ChannelContextHandler channelContextHandler;
  private final CorsHeadersHandler corsHeadersHandler;
  private final GatewayHttpMessageHandler messageHandler;

  public GatewayHttpChannelInitializer(GatewayHttpServer.Config config,
      Consumer<ChannelContext> channelContextConsumer) {
    this.config = config;
    this.channelContextHandler = new ChannelContextHandler(channelContextConsumer);
    this.corsHeadersHandler = new CorsHeadersHandler(config);
    this.messageHandler = new GatewayHttpMessageHandler();
  }

  @Override
  protected void initChannel(Channel channel) {
    ChannelPipeline pipeline = channel.pipeline();
    // contexs contexts contexs
    channel.pipeline().addLast(channelContextHandler);
    // set ssl if present
    if (config.getSslContext() != null) {
      SSLEngine sslEngine = config.getSslContext().createSSLEngine();
      sslEngine.setUseClientMode(false);
      pipeline.addLast(new SslHandler(sslEngine));
    }
    // add http codecs
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(config.getMaxFrameLength(), true));
    // add CORS handler
    if (config.isCorsEnabled()) {
      pipeline.addLast(corsHeadersHandler);
    }
    // message acceptor
    pipeline.addLast(messageHandler);
    // at-least-something exception handler
    pipeline.addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Exception caught for channel {}, {}", ctx.channel(), cause.getMessage(), cause);
      }
    });
  }
}
