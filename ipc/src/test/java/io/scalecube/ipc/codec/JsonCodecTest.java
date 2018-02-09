package io.scalecube.ipc.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.scalecube.ipc.ServiceMessage;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public class JsonCodecTest {

  private static final String QUALIFIER = "io'scalecube'services'transport'\\/Request\\/日本語";

  private static final String STREAM_ID = "君が代";

  private final String sourceJson = "{" +
      "\"data\":{" +
      "   \"favoriteSong\":\"Нино\'С\'Кем\'Ты пьёшь вино\"," +
      "   \"qualifier\":\"hack\"," +
      "   \"contextId\":\"hack\"," +
      "   \"data\":{[\"yadayada\",\"yadayada\",\"yadayada\",\"yadayada\"]}," +
      "   \"language\":\"en\"," +
      "   \"qualifier\":\"hack\"," +
      "   \"contextId\":\"hack\"," +
      "   \"_type\":\"io.scalecube.services.transport.Request\"" +
      "}," +
      "\"qualifier\":\"" + QUALIFIER + "\"," +
      "\"streamId\":\"" + STREAM_ID + "\"}";

  private ServiceMessage.Builder messageBuilder;

  private BiConsumer<String, Object> consumer;

  @Before
  public void setup() {
    messageBuilder = ServiceMessage.builder();
    consumer = (headerName, value) -> {
      switch (headerName) {
        case ServiceMessage.QUALIFIER_NAME:
          messageBuilder = messageBuilder.qualifier((String) value);
          break;
        case ServiceMessage.STREAM_ID_NAME:
          messageBuilder = messageBuilder.streamId((String) value);
          break;
        case ServiceMessage.DATA_NAME:
          messageBuilder = messageBuilder.data(value);
      }
    };
  }

  @Test
  public void testParseSimple() {
    String sourceJson = "{\"key\":\"1234\"}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("key");
    JsonCodec.decode(bb, get, Collections.emptyList(), (headerName, value) -> {
      if ("key".equals(headerName)) {
        messageBuilder = messageBuilder.qualifier((String) value);
      }
    });
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("1234", message.getQualifier());
  }

  @Test
  public void testParse() {
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("streamId", "qualifier");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("io'scalecube'services'transport'/Request/日本語", message.getQualifier());
    Assert.assertEquals(STREAM_ID, message.getStreamId());
  }

  @Test
  public void testParseAndCheckData() {
    String sourceJson = "{" +
        "\"data\":{" +
        "\"contextId\":\"hack\"," +
        "\"data\":{[\"yadayada\", 1, 1e-005]}," +
        "\"qualifier\":\"hack\"" +
        "}";

    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("nothing");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, (headerName, value) -> {
      switch (headerName) {
        case ServiceMessage.DATA_NAME:
          messageBuilder = messageBuilder.data(((ByteBuf) value).toString(CharsetUtil.UTF_8));
          break;
      }
    });
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("{\"contextId\":\"hack\",\"data\":{[\"yadayada\", 1, 1e-005]},\"qualifier\":\"hack\"}",
        message.getData());
  }

  @Test
  public void testParseAndCheckDataTrickyCurlyBrace() {
    String sourceJson = "{" +
        "\"data\":{" +
        "\"contextId\":\"hack\"," +
        "\"data\":{[\"ya{da}}\",\"ya{da}}\"]}," +
        "\"qualifier\":\"hack\"" +
        "}";

    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("nothing");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, (headerName, value) -> {
      switch (headerName) {
        case ServiceMessage.DATA_NAME:
          messageBuilder = messageBuilder.data(((ByteBuf) value).toString(CharsetUtil.UTF_8));
          break;
      }
    });
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("{\"contextId\":\"hack\",\"data\":{[\"ya{da}}\",\"ya{da}}\"]},\"qualifier\":\"hack\"}",
        message.getData());
  }

  @Test
  public void testParseCrappyHeaders() {
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("\"contextId\"", "\"qualifier\"", "\"streamId\"");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertNull(message.getQualifier());
    assertNull(message.getStreamId());
  }

  @Test
  public void testParseUnexistentHeaders() {
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("\"");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, (headerName, value) -> {
      switch (headerName) {
        case "\"":
          fail();
      }
    });
  }

  @Test(expected = IllegalStateException.class)
  public void testParseWithoutDataAndWithUnescapedDoubleQuotesInsideFail() {
    String sourceJson = "{\"streamId\":\"" + STREAM_ID + "\"\"}";

    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("streamId");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, consumer);
  }

  @Test
  public void testParseWithoutDataAndWithEscapedDoubleQuotes() {
    String sourceJson = "{\"streamId\":\"" + STREAM_ID + "\\\"\\\"\\\"" + "\"}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("streamId");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(bb, get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals(STREAM_ID + "\"\"\"", message.getStreamId());
  }

  @Test(expected = IllegalStateException.class)
  public void testParseInvalidColonPlacement() {
    String sourceJson = "{\"streamId\":\"cool\" :}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("streamId", "streamId");
    JsonCodec.decode(bb, get, Collections.emptyList(), consumer);
    fail("IllegalStateException should be thrown in case of invalid colon placement");
  }

  @Test
  public void testParseInsignificantSymbols() {
    String sourceJson = "{\n\r\t \"streamId\"\n\r\t :\n\r\t \"cool\"\n\r\t }";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("streamId");
    JsonCodec.decode(bb, get, Collections.emptyList(), consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("Whitespaces before and after colon should be ignored", "cool", message.getStreamId());
  }

  @Test
  public void testParseEscapedCharacters() {
    String sourceJson = "{\"streamId\":\"\\\"quoted\\\"\"}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("streamId");
    JsonCodec.decode(bb, get, Collections.emptyList(), consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("Parsed string doesn't match source one", "\"quoted\"", message.getStreamId());
  }

  @Test
  public void testSkipExtraFields() {
    String sourceJson = "{\"extra\":\"1234\"," + "\"qualifier\":\"" + 123 + "\"}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("qualifier");
    JsonCodec.decode(bb, get, Collections.emptyList(), consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("Parsed string doesn't match source one", "123", message.getQualifier());
  }

  @Test
  public void testParseEscapeSymbolsInData() {
    String sourceJson = "{\"data\":\"\\u1234\"," + "\"qualifier\":\"" + 123 + "\"}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("qualifier");
    List<String> match = ImmutableList.of("data");

    JsonCodec.decode(bb, get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("Parsed string doesn't match source one", "123", message.getQualifier());
    assertEquals("Data is not equal", "\"\\u1234\"", ((ByteBuf) message.getData()).toString(CharsetUtil.UTF_8));
  }

  @Test
  public void testParseEscapedHexSymbol() {
    String sourceJson = "{\"qualifier\":\"\\uCEB1\\u0061\\u0063\\u006B\\uaef1\"," +
        "\"streamId\":\"\\u0068\\u0061\\u0063\\u006b\"}";
    ByteBuf bb = Unpooled.copiedBuffer(sourceJson.getBytes());
    List<String> get = ImmutableList.of("qualifier", "streamId");
    JsonCodec.decode(bb, get, Collections.emptyList(), consumer);
    ServiceMessage message = messageBuilder.build();
    Assert.assertEquals("Parsed string doesn't match source one", "캱ack껱", message.getQualifier());
    Assert.assertEquals("Parsed string doesn't match source one", "hack", message.getStreamId());
  }

  @Test
  public void testEncodeTwoFlatFieldsMessage() {
    ByteBuf bb = Unpooled.buffer();
    ServiceMessage message = ServiceMessage.withQualifier("/very/cool/qualifier").streamId("stream_id").build();
    JsonCodec.encode(bb, ImmutableList.of("qualifier", "streamId"), Collections.emptyList(),
        headerName -> {
          switch (headerName) {
            case "qualifier":
              return message.getQualifier();
            case "streamId":
              return message.getStreamId();
            default:
              throw new IllegalArgumentException();
          }
        });

    ByteBuf sourceBuf = Unpooled.copiedBuffer(bb);
    ServiceMessage.Builder builder = ServiceMessage.builder();
    JsonCodec.decode(sourceBuf, ImmutableList.of("qualifier", "streamId"), Collections.emptyList(),
        (headerName, obj) -> {
          switch (headerName) {
            case "qualifier":
              builder.qualifier((String) obj);
            case "streamId":
              builder.streamId((String) obj);
          }
        });

    Assert.assertEquals(message, builder.build());
  }

  @Test
  public void testEncodeWithDataFieldMessage() {
    ByteBuf dataBuf = Unpooled.copiedBuffer("{[\"yadayada\",\"yadayada\"]}", StandardCharsets.UTF_8);
    ServiceMessage message = ServiceMessage.withQualifier("/q").data(Unpooled.copiedBuffer(dataBuf)).build();

    ByteBuf bb = Unpooled.buffer();
    JsonCodec.encode(bb, ImmutableList.of("qualifier"), ImmutableList.of("data"),
        headerName -> {
          switch (headerName) {
            case "qualifier":
              return message.getQualifier();
            case "data":
              return message.getData();
            default:
              throw new IllegalArgumentException();
          }
        });

    ServiceMessage.Builder builder = ServiceMessage.builder();
    ByteBuf sourceBuf = Unpooled.copiedBuffer(bb);
    JsonCodec.decode(sourceBuf, ImmutableList.of("qualifier"), ImmutableList.of("data"),
        (headerName, obj) -> {
          switch (headerName) {
            case "qualifier":
              builder.qualifier((String) obj);
            case "data":
              builder.data(obj);
          }
        });

    assertEquals(dataBuf.toString(StandardCharsets.UTF_8),
        ((ByteBuf) builder.build().getData()).toString(StandardCharsets.UTF_8));
  }
}
