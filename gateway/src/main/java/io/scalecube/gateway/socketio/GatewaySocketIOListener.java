package io.scalecube.gateway.socketio;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.ServiceMessage;
import io.scalecube.ipc.codec.ServiceMessageCodec;
import io.scalecube.ipc.netty.ChannelSupport;
import io.scalecube.socketio.Session;
import io.scalecube.socketio.SocketIOListener;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public final class GatewaySocketIOListener implements SocketIOListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewaySocketIOListener.class);

  private final Consumer<ChannelContext> channelContextConsumer;

  private final ConcurrentMap<String, String> sessionIdToChannelContextId = new ConcurrentHashMap<>();

  public GatewaySocketIOListener(Consumer<ChannelContext> channelContextConsumer) {
    this.channelContextConsumer = channelContextConsumer;
  }

  @Override
  public void onConnect(Session session) {
    String channelContextId = IdGenerator.generateId();
    sessionIdToChannelContextId.put(session.getSessionId(), channelContextId);

    ChannelContext channelContext = ChannelContext.create(channelContextId, session.getRemoteAddress());
    channelContextConsumer.accept(channelContext);

    channelContext.listenMessageWrite().subscribe(event -> {
      ServiceMessage message = event.getMessage().get();
      ByteBuf buf = ServiceMessageCodec.encode(message);
      ChannelSupport.releaseRefCount(message.getData()); // release ByteBuf
      try {
        session.send(buf);
        channelContext.postWriteSuccess(message);
      } catch (Exception throwable) {
        channelContext.postWriteError(throwable, message);
      }
    }, throwable -> {
      LOGGER.error("Fatal exception occured on channel context: {}, cause: {}", channelContext.getId(), throwable);
      session.disconnect();
    });
  }

  @Override
  public void onMessage(Session session, ByteBuf buf) {
    String channelContextId = sessionIdToChannelContextId.get(session.getSessionId());
    if (channelContextId == null) {
      LOGGER.error("Can't find channel context id by session id: {}", session.getSessionId());
      ChannelSupport.releaseRefCount(buf);
      session.disconnect();
      return;
    }

    ChannelContext channelContext = ChannelContext.getIfExist(channelContextId);
    if (channelContext == null) {
      ChannelSupport.releaseRefCount(buf);
      LOGGER.error("Failed to handle message, channel context is null by id: {}", channelContextId);
      session.disconnect();
      return;
    }

    try {
      channelContext.postReadSuccess(ServiceMessageCodec.decode(buf));
    } catch (Exception throwable) {
      ChannelSupport.releaseRefCount(buf);
      channelContext.postReadError(throwable);
    }
  }

  @Override
  public void onDisconnect(Session session) {
    String channelContextId = sessionIdToChannelContextId.remove(session.getSessionId());
    if (channelContextId == null) {
      LOGGER.error("Can't find channel context id by session id: {}", session.getSessionId());
      return;
    }
    ChannelContext.closeIfExist(channelContextId);
  }
}
