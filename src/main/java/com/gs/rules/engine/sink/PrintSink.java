package com.gs.rules.engine.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class PrintSink extends BaseSink {

  public PrintSink() {
  }

  @Override
  public void toSink(DataStream<Row> dataStream) {
    dataStream.print();
  }
}
