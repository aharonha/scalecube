package io.scalecube.ipc;

import static io.scalecube.ipc.netty.NettyBootstrapFactory.clientBootstrap;
import static io.scalecube.ipc.netty.NettyBootstrapFactory.serverBootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

public abstract class MessageStream {

  private final Subject<Event, Event> eventSubject = PublishSubject.<Event>create().toSerialized();

  //// Factory

  public static ServerStream newServerStream() {
    return new ServerStream();
  }

  public static ServerStream bindServerStream() {
    return bindServerStream(serverBootstrap());
  }

  public static ServerStream bindServerStream(ServerBootstrap serverBootstrap) {
    return new ServerStream(serverBootstrap, ServerStreamConfig.defaultConfig());
  }

  public static ClientStream newClientStream() {
    return newClientStream(clientBootstrap());
  }

  public static ClientStream newClientStream(Bootstrap clientBootstrap) {
    return new ClientStream(clientBootstrap);
  }

  //// Methods

  public final void subscribe(ChannelContext channelContext) {
    channelContext.subscribe(eventSubject);
  }

  public abstract void close();

  public Observable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess);
  }

  public Observable<Event> listenReadError() {
    return listen().filter(Event::isReadError);
  }

  public Observable<Event> listenWriteSuccess() {
    return listen().filter(Event::isWriteSuccess);
  }

  public Observable<Event> listenWriteError() {
    return listen().filter(Event::isWriteError);
  }

  protected Observable<Event> listen() {
    return eventSubject.onBackpressureBuffer().asObservable();
  }
}
