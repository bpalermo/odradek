package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.io.IOException;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

/** The address a replica advertises: what the other replicas hand to every sidecar. */
class DeliveryServerTest {
  private static final Dispatch UNSET = Dispatch.getDefaultInstance();

  @Test
  void theConfiguredAddressIsAdvertisedAsItIs() {
    Dispatch dispatch =
        Dispatch.newBuilder().setAdvertisedAddress("dispatcher-0.example:7000").build();
    assertThat(DeliveryServer.advertised(dispatch, null, 9465))
        .isEqualTo("dispatcher-0.example:7000");
  }

  @Test
  void theHostOfTheEnvironmentIsAdvertisedWithThePortBeingServed() {
    // As the downward API gives status.podIP, which a shared configuration file cannot.
    assertThat(DeliveryServer.advertised(UNSET, "10.1.2.3", 9465)).isEqualTo("10.1.2.3:9465");
    assertThat(DeliveryServer.advertised(UNSET, " 10.1.2.3\n", 41234)).isEqualTo("10.1.2.3:41234");
    assertThat(DeliveryServer.advertised(UNSET, "pod-0.dispatcher.svc", 9465))
        .isEqualTo("pod-0.dispatcher.svc:9465");
    // An IPv6 pod IP comes without brackets.
    assertThat(DeliveryServer.advertised(UNSET, "fd00::7", 9465)).isEqualTo("[fd00::7]:9465");
    assertThat(DeliveryServer.advertised(UNSET, "[fd00::7]", 9465)).isEqualTo("[fd00::7]:9465");
  }

  @Test
  void withNeitherTheInterfaceAddressIsAdvertised() {
    String expected = DeliveryServer.hostAndPort(DeliveryServer.interfaceAddress(), 9465);
    assertThat(DeliveryServer.advertised(UNSET, null, 9465)).isEqualTo(expected);
    assertThat(DeliveryServer.advertised(UNSET, "  ", 9465)).isEqualTo(expected);
    assertThat(expected).endsWith(":9465");
  }

  @Test
  void anIpv6AddressIsBracketedAndLosesItsScope() throws Exception {
    assertThat(DeliveryServer.hostAndPort(InetAddress.getByName("fd00::7"), 9465))
        .isEqualTo("[fd00:0:0:0:0:0:0:7]:9465");
    assertThat(DeliveryServer.hostAndPort(InetAddress.getByName("10.0.0.7"), 1))
        .isEqualTo("10.0.0.7:1");
  }

  @Test
  void anAddressInTheConfigurationAndAHostInTheEnvironmentIsAMistake() {
    // One is shared by every replica and the other is this replica's own: nothing says which
    // was meant, and a wrong address sends every sidecar to the wrong place.
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters("main", Cluster.getDefaultInstance())
            .setDispatch(Dispatch.newBuilder().setPort(0).setAdvertisedAddress("10.0.0.7:9465"))
            .build();
    DispatchService service = new DispatchService(config, TlsSources.none(), ServerMetrics.noop());
    assertThatThrownBy(() -> DeliveryServer.start(config.getDispatch(), service, null, "10.1.2.3"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("dispatch.advertisedAddress")
        .hasMessageContaining(DeliveryServer.HOST_VARIABLE);
  }
}
