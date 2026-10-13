package io.github.bpalermo.odradek.server.receive;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.server.receive.Holdings.Announcement;
import io.github.bpalermo.odradek.server.receive.Holdings.Partition;
import io.github.bpalermo.odradek.server.receive.Holdings.Range;
import io.github.bpalermo.odradek.server.receive.Holdings.Relay;
import java.util.List;
import org.junit.jupiter.api.Test;

class HoldingsTest {
  private static final long STREAM = 7;
  private static final long OTHER = 8;

  private final Holdings holdings = new Holdings();

  /** Hands the app records of orders/0 from a stream; returns the epoch they go under. */
  private long hand(long stream, long... offsets) {
    return hand("orders", 0, stream, offsets);
  }

  private long hand(String topic, int partition, long stream, long... offsets) {
    Announcement announcement = holdings.arriving(topic, partition, stream);
    for (long offset : offsets) {
      holdings.handed(topic, partition, offset, 0);
    }
    return announcement.epoch();
  }

  // -------------------------------------------------------------------------------------------
  // Announcements and epochs
  // -------------------------------------------------------------------------------------------

  @Test
  void aPartitionIsAnnouncedWhenItsFirstRecordArrives() {
    assertThat(holdings.arriving("orders", 0, STREAM)).isEqualTo(new Announcement(false, true, 1));
    holdings.handed("orders", 0, 5, 0);
    assertThat(holdings.arriving("orders", 0, STREAM))
        .as("the next records need no announcement")
        .isEqualTo(new Announcement(false, false, 1));
  }

  @Test
  void everyAnnouncementHasAnEpochOfItsOwn() {
    assertThat(holdings.arriving("orders", 0, STREAM).epoch()).isEqualTo(1);
    assertThat(holdings.arriving("orders", 1, STREAM).epoch()).isEqualTo(2);
    assertThat(holdings.arriving("refunds", 0, OTHER).epoch()).isEqualTo(3);
  }

  @Test
  void aReleasedPartitionIsLostAndComesBackUnderANewEpoch() {
    long first = hand(STREAM, 1, 2);
    assertThat(holdings.released(STREAM, "orders", List.of(0, 4))).containsExactly(0);
    assertThat(holdings.held()).isZero();
    assertThat(holdings.ack("orders", 0, first, 1, 2)).as("what was held is gone").isNull();

    Announcement again = holdings.arriving("orders", 0, STREAM);
    assertThat(again).isEqualTo(new Announcement(false, true, 2));
    holdings.handed("orders", 0, 1, 0);
    assertThat(holdings.ack("orders", 0, first, 1, 1)).as("the old epoch").isNull();
    assertThat(holdings.ack("orders", 0, again.epoch(), 1, 1))
        .isEqualTo(new Relay(STREAM, List.of(new Range(1, 1))));
  }

  @Test
  void aReleaseFromAStreamThatDoesNotHoldThePartitionChangesNothing() {
    long epoch = hand(STREAM, 1);
    assertThat(holdings.released(OTHER, "orders", List.of(0))).isEmpty();
    assertThat(holdings.released(STREAM, "refunds", List.of(0))).isEmpty();
    assertThat(holdings.ack("orders", 0, epoch, 1, 1)).isNotNull();
  }

  @Test
  void aStreamThatEndsLosesEveryPartitionItDelivered() {
    hand("orders", 1, STREAM, 3);
    hand("orders", 0, STREAM, 9);
    long kept = hand("orders", 2, OTHER, 4);
    assertThat(holdings.gone(STREAM))
        .containsExactly(new Partition("orders", 0), new Partition("orders", 1));
    assertThat(holdings.held()).isEqualTo(1);
    assertThat(holdings.ack("orders", 2, kept, 4, 4)).isNotNull();
    assertThat(holdings.gone(STREAM)).isEmpty();
  }

  @Test
  void aPartitionThatArrivesOnAnotherStreamChangedHands() {
    long first = hand(STREAM, 1, 2);
    // Another replica owns the partition now, whether or not the first has said so.
    Announcement moved = holdings.arriving("orders", 0, OTHER);
    assertThat(moved).isEqualTo(new Announcement(true, true, 2));
    holdings.handed("orders", 0, 1, 0);
    assertThat(holdings.held()).as("what the first stream delivered is dropped").isEqualTo(1);
    assertThat(holdings.ack("orders", 0, first, 1, 2)).isNull();
    assertThat(holdings.ack("orders", 0, moved.epoch(), 1, 2))
        .isEqualTo(new Relay(OTHER, List.of(new Range(1, 1))));

    // The first replica's release, arriving late, is about nothing held any more.
    assertThat(holdings.released(STREAM, "orders", List.of(0))).isEmpty();
  }

  // -------------------------------------------------------------------------------------------
  // Acknowledgements
  // -------------------------------------------------------------------------------------------

  @Test
  void anAcknowledgementGoesToTheStreamTheRecordsCameFrom() {
    long orders = hand("orders", 0, STREAM, 1, 2, 3);
    long refunds = hand("refunds", 0, OTHER, 1);
    assertThat(holdings.ack("orders", 0, orders, 1, 3))
        .isEqualTo(new Relay(STREAM, List.of(new Range(1, 3))));
    assertThat(holdings.ack("refunds", 0, refunds, 1, 1))
        .isEqualTo(new Relay(OTHER, List.of(new Range(1, 1))));
    assertThat(holdings.held()).isZero();
  }

  @Test
  void aRangeOverGapsAcknowledgesOnlyWhatWasDeliveredHere() {
    // The app holds 3, 7, 8 and 12; the offsets in between went to other pods.
    long epoch = hand(STREAM, 3, 7, 8, 12);
    assertThat(holdings.ack("orders", 0, epoch, 0, 10))
        .isEqualTo(new Relay(STREAM, List.of(new Range(3, 3), new Range(7, 8))));
    assertThat(holdings.held()).isEqualTo(1);
    assertThat(holdings.ack("orders", 0, epoch, 4, 6)).as("nothing of it was here").isNull();
    assertThat(holdings.ack("orders", 0, epoch, 12, 12))
        .isEqualTo(new Relay(STREAM, List.of(new Range(12, 12))));
  }

  @Test
  void aRecordIsAcknowledgedOnce() {
    long epoch = hand(STREAM, 1, 2);
    assertThat(holdings.ack("orders", 0, epoch, 1, 1)).isNotNull();
    assertThat(holdings.ack("orders", 0, epoch, 1, 1)).isNull();
    assertThat(holdings.ack("orders", 0, epoch, 1, 2))
        .isEqualTo(new Relay(STREAM, List.of(new Range(2, 2))));
  }

  @Test
  void anAcknowledgementForWhatIsNotHeldIsDropped() {
    long epoch = hand(STREAM, 5);
    assertThat(holdings.ack("orders", 1, epoch, 5, 5)).as("a partition not announced").isNull();
    assertThat(holdings.ack("refunds", 0, epoch, 5, 5)).as("another topic").isNull();
    assertThat(holdings.ack("orders", 0, epoch + 1, 5, 5)).as("an epoch never given").isNull();
    assertThat(holdings.ack("orders", 0, epoch, 6, 5)).as("an empty range").isNull();
    assertThat(holdings.held()).isEqualTo(1);
  }

  @Test
  void aRecordDeliveredAgainIsHeldOnceAndAcknowledgedOnce() {
    long epoch = hand(STREAM, 5, 9);
    // The dispatcher's deadline passed and it sent 5 here again, behind 9.
    assertThat(hand(STREAM, 5)).isEqualTo(epoch);
    assertThat(holdings.held()).isEqualTo(2);
    assertThat(holdings.ack("orders", 0, epoch, 5, 5))
        .isEqualTo(new Relay(STREAM, List.of(new Range(5, 5))));
    assertThat(holdings.ack("orders", 0, epoch, 5, 5)).isNull();
  }

  // -------------------------------------------------------------------------------------------
  // How long the app has held a record
  // -------------------------------------------------------------------------------------------

  @Test
  void theOldestRecordIsTheOneHandedFirstWhateverItsOffset() {
    holdings.arriving("orders", 0, STREAM);
    holdings.handed("orders", 0, 9, 100);
    holdings.handed("orders", 0, 4, 200);
    holdings.arriving("orders", 1, STREAM);
    holdings.handed("orders", 1, 1, 300);
    assertThat(holdings.oldestHandedNanos()).isEqualTo(100);

    holdings.ack("orders", 0, 1, 9, 9);
    assertThat(holdings.oldestHandedNanos()).isEqualTo(200);
    holdings.ack("orders", 0, 1, 4, 4);
    assertThat(holdings.oldestHandedNanos()).isEqualTo(300);
    holdings.released(STREAM, "orders", List.of(1));
    assertThat(holdings.held()).isZero();
  }

  @Test
  void aRecordHandedAgainIsHeldFromThen() {
    holdings.arriving("orders", 0, STREAM);
    holdings.handed("orders", 0, 4, 100);
    holdings.handed("orders", 0, 6, 150);
    holdings.handed("orders", 0, 4, 200);
    assertThat(holdings.oldestHandedNanos()).isEqualTo(150);
  }
}
