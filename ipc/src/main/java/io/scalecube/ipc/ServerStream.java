package io.scalecube.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

public final class ServerStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);

  private final Subject<Event, Event> eventSubject = PublishSubject.<Event>create().toSerialized();

  public void subscribe(ChannelContext channelContext) {
    channelContext.subscribe(eventSubject);
  }

  public void send(ServiceMessage message) {
    ServerStreamUtil.prepareMessageOnSend(message, (identity, message1) -> {
      ChannelContext channelContext = ChannelContext.getIfExist(identity);
      if (channelContext != null) {
        channelContext.postMessageWrite(message1);
        return;
      }
      LOGGER.warn("Failed to handle message: {}, channel context is null by id: {}", message, identity);
      // Hint: at this point plugin fallback behaviour when msg can't be sent
    }, throwable -> {
      LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable);
      // Hint: at this point plugin fallback behaviour when msg can't be sent
    });
  }

  public Observable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess).map(event -> {
      ServiceMessage message = event.getMessage().get();
      ServiceMessage message1 = ServerStreamUtil.prepareMessageOnReceive(message, event.getIdentity());
      return Event.copyFrom(event).message(message1).build();
    });
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
