package hbase;

import org.apache.hadoop.hbase.Stoppable;


public class StoppableImplementation implements Stoppable {
  volatile boolean stopped = false;

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }
}
