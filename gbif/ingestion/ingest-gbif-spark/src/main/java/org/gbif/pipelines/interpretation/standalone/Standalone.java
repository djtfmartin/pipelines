package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class Standalone {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      throw new IllegalArgumentException("Expecting two arguments: <mode> <config-file>");
    }

    Mode mode = Mode.valueOf(args[0].toUpperCase());
    PipelinesConfig config = loadConfig(args[1]);
    switch (mode) {
      case INTERPRETATION:
        new InterpretationStandalone().start(config);
        break;
      case IDENTIFIER:
        new IdentifierStandalone().start(config);
        break;
      case HDFS_VIEW:
        new HdfsViewStandalone().start(config);
        break;
      default:
        throw new IllegalArgumentException("Unknown mode: " + args[0]);
    }
  }

  public enum Mode {
    INTERPRETATION,
    IDENTIFIER,
    HDFS_VIEW
  }
}
