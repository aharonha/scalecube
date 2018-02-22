package io.scalecube.ipc;

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
    return new ServerStream(ServerStreamConfig.defaultConfig(), null);
  }

  public static ServerStream bindServerStream(ServerStreamConfig config, ServerBootstrap serverBootstrap) {
    return new ServerStream(config, serverBootstrap);
  }

  public static ClientStream newClientStream() {
    return new ClientStream(null);
  }

  public static ClientStream newClientStream(Bootstrap bootstrap) {
    return new ClientStream(bootstrap);
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
