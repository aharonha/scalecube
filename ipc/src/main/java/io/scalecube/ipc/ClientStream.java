package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.CompletableFuture;

public final class ClientStream {

  private final Subject<Event, Event> eventSubject = PublishSubject.<Event>create().toSerialized();

  private final NettyClientTransport clientTransport;

  public ClientStream() {
    // Hint: instead of hardcoded use of netty transport - abstract transport with interface and load concrete transport
    // implementation from META-INF/services
    this.clientTransport = new NettyClientTransport(channelContext -> channelContext.subscribe(eventSubject));
  }

  public void send(Address address, ServiceMessage message) {
    CompletableFuture<ChannelContext> promise = clientTransport.getOrConnect(address);
    promise.whenComplete((channelContext, throwable) -> {
      if (channelContext != null) {
        channelContext.postMessageWrite(message);
      }
    });
  }

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

  private Observable<Event> listen() {
    return eventSubject.onBackpressureBuffer().asObservable();
  }
}
