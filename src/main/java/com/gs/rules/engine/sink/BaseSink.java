package com.gs.rules.engine.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.Serializable;

public abstract class BaseSink implements Serializable {

  public abstract void toSink(DataStream<Row> dataStream);
}
