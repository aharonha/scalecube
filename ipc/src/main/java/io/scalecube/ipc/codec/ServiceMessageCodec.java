package io.scalecube.ipc.codec;

import io.scalecube.ipc.ServiceMessage;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;

import java.util.List;

public final class ServiceMessageCodec {

  private static final List<String> FLAT_FIELDS = ImmutableList.of(
      ServiceMessage.QUALIFIER_NAME, ServiceMessage.SENDER_ID_NAME, ServiceMessage.STREAM_ID_NAME);
  private static final List<String> MATCH_FIELDS = ImmutableList.of(ServiceMessage.DATA_NAME);

  private static final ByteBufAllocator ALLOCATOR = ByteBufAllocator.DEFAULT;

  private ServiceMessageCodec() {
    // Do not instantiate
  }

  public static ServiceMessage decode(ByteBuf buf) {
    ServiceMessage.Builder messageBuilder = ServiceMessage.builder();
    try {
      JsonCodec.decode(buf.slice(), FLAT_FIELDS, MATCH_FIELDS, (fieldName, value) -> {
        switch (fieldName) {
          case ServiceMessage.QUALIFIER_NAME:
            messageBuilder.qualifier((String) value);
            break;
          case ServiceMessage.SENDER_ID_NAME:
            messageBuilder.senderId((String) value);
            break;
          case ServiceMessage.STREAM_ID_NAME:
            messageBuilder.streamId((String) value);
            break;
          case ServiceMessage.DATA_NAME:
            messageBuilder.data(value); // ByteBuf
            break;
          default:
            // no-op
        }
      });
    } catch (Exception e) {
      throw new DecoderException(e);
    }
    return messageBuilder.build();
  }

  public static ByteBuf encode(ServiceMessage message) {
    ByteBuf buf = ALLOCATOR.buffer();
    try {
      JsonCodec.encode(buf, FLAT_FIELDS, MATCH_FIELDS, fieldName -> {
        switch (fieldName) {
          case ServiceMessage.QUALIFIER_NAME:
            return message.getQualifier();
          case ServiceMessage.SENDER_ID_NAME:
            return message.getSenderId();
          case ServiceMessage.STREAM_ID_NAME:
            return message.getStreamId();
          case ServiceMessage.DATA_NAME:
            return message.getData(); // ByteBuf
          default:
            throw new IllegalArgumentException(fieldName);
        }
      });
    } catch (Exception e) {
      buf.release(); // buf belongs to this function => he released in this function
      throw new EncoderException(e);
    }
    return buf;
  }
}
