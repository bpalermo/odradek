package io.github.bpalermo.odradek.server.receive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class CreditSplitTest {
  private final CreditSplit split = new CreditSplit();

  /** How many streams there are meant to be. */
  private int wanted;

  private void open(long stream) {
    wanted++;
    split.opened(stream);
  }

  private Map<Long, Long> grants() {
    return split.grants(wanted);
  }

  /** Records arrive on a stream and the app, which has credit, is handed them at once. */
  private void through(long stream, int records) {
    split.received(stream, records);
    split.handed(records);
  }

  @Test
  void nothingIsGrantedBeforeTheAppGrants() {
    open(1);
    assertThat(grants()).isEmpty();
  }

  @Test
  void nothingIsGrantedWithoutAStream() {
    split.appGranted(10);
    assertThat(grants()).isEmpty();
  }

  @Test
  void oneStreamIsPassedTheAppsCredits() {
    open(1);
    split.appGranted(500);
    assertThat(grants()).containsExactly(Map.entry(1L, 500L));
    assertThat(grants()).isEmpty();

    through(1, 200);
    assertThat(grants()).as("nothing new from the app").isEmpty();
    split.appGranted(250);
    assertThat(grants()).containsExactly(Map.entry(1L, 250L));
    assertThat(split.outstanding(1)).isEqualTo(550);
    assertThat(split.appCredit()).isEqualTo(550);
  }

  @Test
  void aStreamOpenedLaterGetsTheCreditsTheAppGrantedBefore() {
    split.appGranted(8);
    assertThat(grants()).isEmpty();
    open(1);
    assertThat(grants()).containsExactly(Map.entry(1L, 8L));
  }

  @Test
  void theCreditOfAStreamThatEndedIsGrantedToTheNext() {
    open(1);
    split.appGranted(8);
    grants();
    split.closed(1);
    wanted--;
    open(2);
    assertThat(grants()).containsExactly(Map.entry(2L, 8L));
  }

  @Test
  void aStreamIsNotPassedEverythingWhileAnotherIsBeingOpenedAgain() {
    open(1);
    open(2);
    split.appGranted(100);
    grants();
    // The second stream is lost and wanted back: the first keeps to its window meanwhile.
    split.closed(2);
    assertThat(grants()).isEmpty();
    through(1, 1);
    assertThat(grants()).containsExactly(Map.entry(1L, 2L));
    split.opened(3);
    assertThat(grants()).containsExactly(Map.entry(3L, 1L));
  }

  @Test
  void severalStreamsStartWithOneCreditEach() {
    open(1);
    open(2);
    open(3);
    split.appGranted(600);
    assertThat(grants()).containsOnly(Map.entry(1L, 1L), Map.entry(2L, 1L), Map.entry(3L, 1L));
    assertThat(grants()).as("the rest waits for records").isEmpty();
  }

  @Test
  void theWindowOfAStreamThatUsesItsCreditDoubles() {
    open(1);
    open(2);
    split.appGranted(400);
    grants();

    through(1, 1);
    assertThat(grants()).containsExactly(Map.entry(1L, 2L));
    through(1, 2);
    assertThat(grants()).containsExactly(Map.entry(1L, 4L));
    through(1, 4);
    assertThat(grants()).containsExactly(Map.entry(1L, 8L));
    assertThat(split.outstanding(2)).as("the idle stream still holds one").isEqualTo(1);
  }

  @Test
  void aWindowThatIsNotUsedUpIsOnlyToppedUp() {
    open(1);
    open(2);
    split.appGranted(400);
    grants();
    through(1, 1);
    grants();
    through(1, 2);
    grants();
    assertThat(split.outstanding(1)).isEqualTo(4);

    through(1, 1);
    assertThat(grants()).as("one used, one given back").containsExactly(Map.entry(1L, 1L));
  }

  @Test
  void aWindowIsAtMostTheBudgetOverTwiceTheStreams() {
    open(1);
    open(2);
    split.appGranted(40);
    grants();
    // The app acknowledges at once, so its budget stays at 40: a window of 40 / (2 * 2).
    for (int round = 0; round < 12; round++) {
      int records = (int) split.outstanding(1);
      through(1, records);
      split.appGranted(records);
      grants();
    }
    assertThat(split.outstanding(1)).isEqualTo(10);
    assertThat(split.outstanding(2)).isEqualTo(1);
  }

  @Test
  void streamsThatWereBusyCannotSitOnMoreThanHalfOfTheBudget() {
    for (long stream = 1; stream <= 4; stream++) {
      open(stream);
    }
    split.appGranted(80);
    grants();
    // Every stream is busy for a while, then all go quiet with their windows full.
    for (int round = 0; round < 12; round++) {
      for (long stream = 1; stream <= 4; stream++) {
        int records = (int) split.outstanding(stream);
        through(stream, records);
        split.appGranted(records);
      }
      grants();
    }
    long sittingOn = 0;
    for (long stream = 1; stream <= 4; stream++) {
      sittingOn += split.outstanding(stream);
    }
    assertThat(sittingOn).isEqualTo(40);
  }

  @Test
  void whatArrivesIsGrantedAgainWhereItArrived() {
    open(1);
    open(2);
    split.appGranted(100);
    grants();
    long toFirst = 0;
    for (int round = 0; round < 20; round++) {
      int records = (int) split.outstanding(1);
      through(1, records);
      toFirst += records;
      grants();
    }
    // The app took 99 of its 100 from the stream that has records, with the other one idle.
    assertThat(toFirst).isEqualTo(99);
    assertThat(split.appCredit()).isEqualTo(1);
    assertThat(split.outstanding(2)).isEqualTo(1);
  }

  @Test
  void anAppThatAsksForOneRecordHearsOfEveryStream() {
    open(1);
    open(2);
    open(3);
    split.appGranted(1);
    // One credit, three streams: each may send one record, or the app would only ever be asked
    // by the stream that happened to get it.
    assertThat(grants()).containsOnly(Map.entry(1L, 1L), Map.entry(2L, 1L), Map.entry(3L, 1L));

    // All three send. The app takes one; two wait, which is the most that can.
    split.received(1, 1);
    split.received(2, 1);
    split.received(3, 1);
    split.handed(1);
    assertThat(split.buffered()).isEqualTo(2);
    assertThat(grants()).as("the app has asked for nothing more").isEmpty();

    // The app asks for one more, twice: it gets what waits, and nothing is granted meanwhile.
    split.appGranted(1);
    split.handed(1);
    assertThat(grants()).isEmpty();
    split.appGranted(1);
    split.handed(1);
    assertThat(grants()).isEmpty();

    split.appGranted(1);
    assertThat(grants()).hasSize(3);
  }

  @Test
  void whatWaitsForTheAppIsNeverMoreThanTheStreamsLessOne() {
    int streams = 5;
    for (long stream = 1; stream <= streams; stream++) {
      open(stream);
    }
    java.util.Random random = new java.util.Random(7);
    for (int step = 0; step < 20_000; step++) {
      switch (random.nextInt(3)) {
        case 0 -> split.appGranted(1 + random.nextInt(3));
        case 1 -> {
          long stream = 1 + random.nextInt(streams);
          long outstanding = split.outstanding(stream);
          if (outstanding > 0) {
            split.received(stream, 1 + random.nextInt((int) Math.min(outstanding, 4)));
          }
        }
        default -> grants();
      }
      long toHand = Math.min(split.buffered(), split.appCredit());
      if (toHand > 0) {
        split.handed((int) toHand);
      }
      assertThat(split.buffered() - split.appCredit())
          .as("records held beyond what the app asked for, at step %s", step)
          .isLessThanOrEqualTo(streams - 1);
    }
  }

  @Test
  void aRecordThatIsDroppedFreesItsPlace() {
    open(1);
    open(2);
    split.appGranted(1);
    grants();
    split.received(1, 1);
    split.received(2, 1);
    split.handed(1);
    assertThat(split.buffered()).isEqualTo(1);
    split.dropped(1);
    split.appGranted(1);
    assertThat(grants()).hasSize(2);
  }
}
