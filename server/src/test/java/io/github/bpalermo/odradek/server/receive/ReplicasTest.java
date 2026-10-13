package io.github.bpalermo.odradek.server.receive;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import org.junit.jupiter.api.Test;

class ReplicasTest {
  private final Replicas replicas = new Replicas();

  private static ReplicaList list(long generation, String... addresses) {
    ReplicaList.Builder list = ReplicaList.newBuilder().setGeneration(generation);
    for (String address : addresses) {
      list.addReplicas(Replica.newBuilder().setAddress(address));
    }
    return list.build();
  }

  @Test
  void nothingIsKnownAtFirst() {
    assertThat(replicas.known()).isFalse();
    assertThat(replicas.addresses()).isEmpty();
  }

  @Test
  void theFirstListIsTakenWhateverItsGeneration() {
    assertThat(replicas.offer(list(0, "10.0.0.1:9465"))).isTrue();
    assertThat(replicas.known()).isTrue();
    assertThat(replicas.addresses()).containsExactly("10.0.0.1:9465");
  }

  @Test
  void aListWithAHigherGenerationReplacesTheOneHeld() {
    replicas.offer(list(4, "10.0.0.1:9465", "10.0.0.2:9465"));
    assertThat(replicas.offer(list(5, "10.0.0.2:9465", "10.0.0.3:9465"))).isTrue();
    assertThat(replicas.addresses()).containsExactly("10.0.0.2:9465", "10.0.0.3:9465");
  }

  @Test
  void aListThatIsNotNewerIsIgnored() {
    replicas.offer(list(5, "10.0.0.1:9465"));
    // The same list arrives on every stream; an older one may still be on its way.
    assertThat(replicas.offer(list(5, "10.0.0.9:9465"))).isFalse();
    assertThat(replicas.offer(list(4, "10.0.0.9:9465"))).isFalse();
    assertThat(replicas.addresses()).containsExactly("10.0.0.1:9465");
  }

  @Test
  void generationsAreUnsigned() {
    replicas.offer(list(Long.MAX_VALUE, "10.0.0.1:9465"));
    assertThat(replicas.offer(list(Long.MIN_VALUE, "10.0.0.2:9465")))
        .as("2^63 is above 2^63 - 1")
        .isTrue();
    assertThat(replicas.offer(list(1, "10.0.0.3:9465"))).isFalse();
  }

  @Test
  void onceForgottenAnyListIsTheFirstAgain() {
    replicas.offer(list(9, "10.0.0.1:9465"));
    replicas.forget();
    assertThat(replicas.known()).isFalse();
    assertThat(replicas.addresses()).isEmpty();
    // A dispatcher that was replaced as a whole counts from where it likes.
    assertThat(replicas.offer(list(1, "10.0.0.7:9465"))).isTrue();
    assertThat(replicas.addresses()).containsExactly("10.0.0.7:9465");
  }

  @Test
  void aListMayBeEmptyAndAReplicaWithoutAnAddressIsNotOne() {
    assertThat(replicas.offer(list(1, "", "10.0.0.1:9465", "10.0.0.1:9465"))).isTrue();
    assertThat(replicas.addresses()).containsExactly("10.0.0.1:9465");
    assertThat(replicas.offer(list(2))).isTrue();
    assertThat(replicas.known()).isTrue();
    assertThat(replicas.addresses()).isEmpty();
  }
}
