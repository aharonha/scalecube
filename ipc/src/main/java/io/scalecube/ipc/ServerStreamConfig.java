package io.scalecube.ipc;

public final class ServerStreamConfig {

  public static final String DEFAULT_LISTEN_ADDRESS = null;
  public static final String DEFAULT_LISTEN_INTERFACE = null; // Default listen settings fallback to getLocalHost
  public static final boolean DEFAULT_PREFER_IP6 = false;
  public static final int DEFAULT_PORT = 5801;
  public static final int DEFAULT_PORT_COUNT = 100;
  public static final boolean DEFAULT_PORT_AUTO_INCREMENT = true;

  private final String listenAddress;
  private final String listenInterface;
  private final boolean preferIPv6;
  private final int port;
  private final int portCount;
  private final boolean portAutoIncrement;

  private ServerStreamConfig(Builder builder) {
    this.listenAddress = builder.listenAddress;
    this.listenInterface = builder.listenInterface;
    this.port = builder.port;
    this.portCount = builder.portCount;
    this.portAutoIncrement = builder.portAutoIncrement;
    this.preferIPv6 = builder.preferIPv6;
  }

  public static ServerStreamConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getListenAddress() {
    return listenAddress;
  }

  public String getListenInterface() {
    return listenInterface;
  }

  public boolean isPreferIPv6() {
    return preferIPv6;
  }

  public int getPort() {
    return port;
  }

  public int getPortCount() {
    return portCount;
  }

  public boolean isPortAutoIncrement() {
    return portAutoIncrement;
  }

  public static class Builder {

    private String listenAddress = DEFAULT_LISTEN_ADDRESS;
    private String listenInterface = DEFAULT_LISTEN_INTERFACE;
    private boolean preferIPv6 = DEFAULT_PREFER_IP6;
    private int port = DEFAULT_PORT;
    private int portCount = DEFAULT_PORT_COUNT;
    private boolean portAutoIncrement = DEFAULT_PORT_AUTO_INCREMENT;

    private Builder() {}

    public Builder listenAddress(String listenAddress) {
      this.listenAddress = listenAddress;
      return this;
    }

    public Builder listenInterface(String listenInterface) {
      this.listenInterface = listenInterface;
      return this;
    }

    public Builder preferIPv6(boolean preferIPv6) {
      this.preferIPv6 = preferIPv6;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder portCount(int portCount) {
      this.portCount = portCount;
      return this;
    }

    public Builder portAutoIncrement(boolean portAutoIncrement) {
      this.portAutoIncrement = portAutoIncrement;
      return this;
    }

    public ServerStreamConfig build() {
      return new ServerStreamConfig(this);
    }
  }
}
