package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic.MessageWrite;
import static io.scalecube.ipc.Event.Topic.ReadError;
import static io.scalecube.ipc.Event.Topic.ReadSuccess;
import static io.scalecube.ipc.Event.Topic.WriteError;
import static io.scalecube.ipc.Event.Topic.WriteSuccess;

import io.scalecube.transport.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ChannelContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelContext.class);

  private static final ConcurrentMap<String, ChannelContext> idToChannelContext = new ConcurrentHashMap<>();
  private static final ConcurrentMap<ChannelContext, String> channelContextToId = new ConcurrentHashMap<>();

  private final Subject<Event, Event> eventSubject = PublishSubject.<Event>create().toSerialized();
  private final Subject<Void, Void> closeSubject = PublishSubject.<Void>create().toSerialized();

  private final String id;
  private final Address address;
  private final String addressAsString;

  private ChannelContext(String id, SocketAddress address) {
    Objects.requireNonNull(id);
    Objects.requireNonNull(address);

    InetSocketAddress address1 = (InetSocketAddress) address;
    if (address1.isUnresolved()) {
      throw new IllegalArgumentException(
          "ChannelContext can't be constructed with unresolved socket address: " + address);
    }

    this.id = id;
    this.address = Address.create(address1.getAddress().getHostAddress(), address1.getPort());
    this.addressAsString = this.address.toString();
  }

  public static ChannelContext create(String id, SocketAddress address) {
    ChannelContext channelContext = new ChannelContext(id, address);
    idToChannelContext.put(id, channelContext);
    channelContextToId.put(channelContext, id);
    LOGGER.debug("Created {} for network session: {}", channelContext, id);
    return channelContext;
  }

  public static ChannelContext getIfExist(String id) {
    return idToChannelContext.get(id);
  }

  public static void closeIfExist(String id) {
    Optional.ofNullable(ChannelContext.getIfExist(id)).ifPresent(ChannelContext::close);
  }

  public String getId() {
    return id;
  }

  public Address getAddress() {
    return address;
  }

  public void subscribe(Subject<Event, Event> eventSubject) {
    listen().subscribe(eventSubject::onNext, this::handleOnError);
  }

  private void handleOnError(Throwable throwable) {
    // at this point eventSubject not listening anymore to this channel context
    LOGGER.error("Unsubscribed {} due to unhandled exception caught: {}", this, throwable);
  }

  private Observable<Event> listen() {
    return eventSubject.onBackpressureBuffer().asObservable();
  }

  public Observable<Void> listenClose() {
    return closeSubject.onBackpressureBuffer().asObservable();
  }

  public Observable<Event> listenMessageReceived() {
    return listen().filter(Event::isReadSuccess);
  }

  public Observable<Event> listenReadError() {
    return listen().filter(Event::isReadError);
  }

  public Observable<Event> listenMessageWrite() {
    return listen().filter(Event::isMessageWrite);
  }

  public void postReadSuccess(ServiceMessage message) {
    eventSubject.onNext(new Event.Builder(ReadSuccess, this).message(message).build());
  }

  public void postReadError(Throwable throwable) {
    eventSubject.onNext(new Event.Builder(ReadError, this).error(throwable).build());
  }

  public void postMessageWrite(ServiceMessage message) {
    eventSubject.onNext(new Event.Builder(MessageWrite, this).message(message).build());
  }

  public void postWriteError(Throwable throwable, ServiceMessage message) {
    eventSubject.onNext(new Event.Builder(WriteError, this).error(throwable).message(message).build());
  }

  public void postWriteSuccess(ServiceMessage message) {
    eventSubject.onNext(new Event.Builder(WriteSuccess, this).message(message).build());
  }

  public void close() {
    String id = channelContextToId.remove(this);
    if (id != null) {
      idToChannelContext.remove(id);
    }
    eventSubject.onCompleted();
    closeSubject.onNext(null);
    closeSubject.onCompleted();
    LOGGER.debug("Removed and closed {} for network session: {}", this, id);
  }

  @Override
  public String toString() {
    return "ChannelContext{" +
        "id=" + id +
        ", address=" + addressAsString +
        '}';
  }
}
