package io.scalecube.ipc;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class ServerStreamUtil {

  public static final String SENDER_ID_DELIMITER = "/";

  public static final Throwable INVALID_IDENTITY_EXCEPTION =
      new IllegalArgumentException("ServiceMessage: invalid identity");

  private ServerStreamUtil() {
    // Do not instantiate
  }

  public static void prepareMessageOnSend(ServiceMessage message,
      BiConsumer<String, ServiceMessage> consumer,
      Consumer<Throwable> throwableConsumer) {

    if (!message.hasSenderId()
        || message.getSenderId().startsWith(SENDER_ID_DELIMITER)
        || message.getSenderId().endsWith(SENDER_ID_DELIMITER)) {
      throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION);
      return;
    }

    String senderId = message.getSenderId();
    String serverId = senderId;
    String newSenderId = null;
    int delimiter = senderId.lastIndexOf(SENDER_ID_DELIMITER);
    if (delimiter > 0) {
      // extract last identity
      serverId = senderId.substring(delimiter + 1);
      if (serverId.isEmpty()) {
        throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION);
        return;
      }
      // construct new sender qualfier
      newSenderId = senderId.substring(0, delimiter);
      if (newSenderId.isEmpty()) {
        throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION);
        return;
      }
    }

    consumer.accept(serverId, ServiceMessage.copyFrom(message).senderId(newSenderId).build());
  }

  public static ServiceMessage prepareMessageOnReceive(ServiceMessage message, String identity) {
    String newSenderId = identity;
    if (message.hasSenderId()) {
      newSenderId = message.getSenderId() + SENDER_ID_DELIMITER + identity;
    }
    return ServiceMessage.copyFrom(message).senderId(newSenderId).build();
  }
}
