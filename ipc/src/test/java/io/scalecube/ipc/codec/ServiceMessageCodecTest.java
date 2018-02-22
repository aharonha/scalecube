package io.scalecube.ipc.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.scalecube.ipc.ServiceMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ServiceMessageCodecTest {

  public static final Charset CHARSET = StandardCharsets.UTF_8;

  @Test
  public void testCodecWithoutAnything() {
    ServiceMessage src = ServiceMessage.builder().build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{}", buf.toString(CHARSET));

    ByteBuf buf1 = buf.copy();
    int ri = buf1.readerIndex();
    ServiceMessage message = ServiceMessageCodec.decode(buf1);
    assertEquals(ri, buf1.readerIndex());
    assertEquals(src, message);
  }

  @Test
  public void testCodecWithOnlyQualifier() {
    ServiceMessage src = ServiceMessage.withQualifier("q").build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\"}", buf.toString(CHARSET));
  }

  @Test
  public void testCodecWithOnlyData() {
    ByteBuf buf_src = Unpooled.copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}".getBytes());
    ServiceMessage src = ServiceMessage.withQualifier((String) null).data(buf_src).build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}", buf.toString(CHARSET));
  }

  @Test
  public void testCodecWithNullData() {
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("stream_id").data(null).build();
    assertEquals("{\"q\":\"q\",\"streamId\":\"stream_id\"}", ServiceMessageCodec.encode(src).toString(CHARSET));
  }

  @Test
  public void testCodecWithEmptyData() {
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("stream_id").data(Unpooled.EMPTY_BUFFER).build();
    assertEquals("{\"q\":\"q\",\"streamId\":\"stream_id\"}", ServiceMessageCodec.encode(src).toString(CHARSET));
  }

  @Test
  public void testCodecWithoutData() {
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("streamId").senderId("senderId").build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\",\"senderId\":\"senderId\",\"streamId\":\"streamId\"}", buf.toString(CHARSET));

    ByteBuf buf1 = buf.copy();
    int ri = buf1.readerIndex();
    ServiceMessage message = ServiceMessageCodec.decode(buf1);
    assertEquals(ri, buf1.readerIndex());
    assertEquals(null, message.getData());
  }

  @Test
  public void testCodecWithByteBufData() {
    ByteBuf buf_src = Unpooled.copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}".getBytes());
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("streamId").data(buf_src).build();

    int ri = buf_src.readerIndex();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\",\"streamId\":\"streamId\",\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}",
        buf.toString(CHARSET));
    assertEquals(ri, buf_src.readerIndex());

    ServiceMessage message = ServiceMessageCodec.decode(buf.copy());
    assertNotNull(message.getData());
    assertEquals("q", message.getQualifier());
    assertEquals("streamId", message.getStreamId());
    assertEquals(buf_src.toString(CHARSET), ((ByteBuf) message.getData()).toString(CHARSET));
  }

  @Test
  public void testCodecWithByteBufDataSurrogated() {
    ByteBuf byteSrc = Unpooled
        .copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1,\"something\":\"\ud83d\ude0c\"}".getBytes());
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("stream_id").data(byteSrc).build();

    ByteBuf bb = ServiceMessageCodec.encode(src);
    String s = bb.toString(CHARSET);
    assertEquals(s.charAt(98), (char) 0xD83D);
    assertEquals(s.charAt(99), (char) 0xDE0C);
  }

  @Test
  public void testCodecWithByteBufDataNoJsonValidation() {
    ByteBuf buf = Unpooled.copiedBuffer("{\"hello\"w{o{r{l{d", Charset.defaultCharset());
    int ri = buf.readerIndex();
    ServiceMessage msg = ServiceMessage.withQualifier("q")
        .streamId("stream_id")
        .data(buf)
        .build();

    assertEquals("{\"q\":\"q\",\"streamId\":\"stream_id\",\"data\":{\"hello\"w{o{r{l{d}",
        ServiceMessageCodec.encode(msg).toString(CHARSET));
    assertEquals(ri, buf.readerIndex());
  }
}
