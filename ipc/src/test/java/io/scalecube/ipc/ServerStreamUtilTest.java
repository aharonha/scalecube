package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ServerStreamUtilTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSetSenderIdOnSendOnNull() {
    expectedException.expect(IllegalArgumentException.class);
    ServiceMessage message = ServiceMessage.builder().senderId(null).build();
    ServerStreamUtil.prepareMessageOnSend(message, (i, m) -> {
    }, e -> {
      throw new IllegalArgumentException(e);
    });
  }

  @Test
  public void testSetSenderIdOnSendOnEmptyString() {
    expectedException.expect(IllegalArgumentException.class);
    ServiceMessage message = ServiceMessage.builder().senderId("").build();
    ServerStreamUtil.prepareMessageOnSend(message, (i, m) -> {
    }, e -> {
      throw new IllegalArgumentException(e);
    });
  }

  @Test
  public void testSetSenderIdOnSendWithDelimiterThenEmpty() {
    expectedException.expect(IllegalArgumentException.class);
    String senderId = "auifas/";
    ServiceMessage message = ServiceMessage.builder().senderId(senderId).build();
    ServerStreamUtil.prepareMessageOnSend(message, (i, m) -> {
    }, e -> {
      throw new IllegalArgumentException(e);
    });
  }

  @Test
  public void testSetSenderIdOnSendBeginsWithDelimiter() {
    expectedException.expect(IllegalArgumentException.class);
    String senderId = "/au/ifas";
    ServiceMessage message = ServiceMessage.builder().senderId(senderId).build();
    ServerStreamUtil.prepareMessageOnSend(message, (i, m) -> {
    }, e -> {
      throw new IllegalArgumentException(e);
    });
  }

  @Test
  public void testSetSenderIdOnSendNoDelimiter() {
    String senderId = "auifasdihfaasd87f2";
    String[] identity = new String[1];
    ServiceMessage[] message = new ServiceMessage[1];
    ServiceMessage message1 = ServiceMessage.builder().senderId(senderId).build();
    ServerStreamUtil.prepareMessageOnSend(message1, (i, m) -> {
      identity[0] = i;
      message[0] = m;
    }, e -> {
      throw new IllegalArgumentException(e);
    });
    assertEquals(senderId, identity[0]);
    assertEquals(null, message[0].getSenderId());
  }

  @Test
  public void testSetSenderIdOnSend() {
    String senderId = "au/ifas/cafe";
    String[] identity = new String[1];
    ServiceMessage[] message = new ServiceMessage[1];
    ServiceMessage message1 = ServiceMessage.builder().senderId(senderId).build();
    ServerStreamUtil.prepareMessageOnSend(message1, (i, m) -> {
      identity[0] = i;
      message[0] = m;
    }, e -> {
      throw new IllegalArgumentException(e);
    });
    assertEquals("cafe", identity[0]);
    assertEquals("au/ifas", message[0].getSenderId());
  }

  @Test
  public void testSetSenderIdOnReceiveEmptyAtStart() {
    ServiceMessage message = ServiceMessage.builder().build();
    String identity = "id";
    assertEquals(identity, ServerStreamUtil.prepareMessageOnReceive(message, identity).getSenderId());
  }

  @Test
  public void testSetSenderIdOnReceiveNotEmptyAtStart() {
    ServiceMessage message = ServiceMessage.builder().senderId("1/2/3").build();
    String identity = "4";
    assertEquals("1/2/3/4", ServerStreamUtil.prepareMessageOnReceive(message, identity).getSenderId());
  }
}
