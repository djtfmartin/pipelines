package org.gbif.pipelines.interpretation.parsers.temporal.parser;

import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.interpretation.parsers.temporal.utils.DelimiterUtils;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Interpreter for raw time. The main method parse
 */
class ParserRawTime {

  private ParserRawTime() {
    //NOP
  }

  public static ChronoAccumulator parse(String rawTime) {
    ChronoAccumulator accumulator = new ChronoAccumulator();
    if (isEmpty(rawTime)) {
      return accumulator;
    }
    //Split by some zone char
    String[] timeArray = DelimiterUtils.splitTime(rawTime);

    //Parse time only
    if (timeArray.length > 1) {
      accumulator.put(HOUR_OF_DAY, timeArray[0]);
      accumulator.put(MINUTE_OF_HOUR, timeArray[1]);
      if (timeArray.length > 2) {
        accumulator.put(SECOND_OF_MINUTE, timeArray[2]);
      }
    }

    return accumulator;
  }

}

