/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Adaptor that is able to collect system metrics by using Hyperic Sigar.
 * <P>
 * This adaptor is added to an Agent like so:
 * <code>
 * add SystemMetrics [dataType] [seconds]
 * </code>
 * <ul>
 * <li><code>dataType</code> - The chukwa data type, use SystemMetrics to map to 
 * default SystemMetrics demux parser.</li>
 * <li><code>seconds</code> - Interval to collect system metrics, default is 60 seconds.</li>
 * </ul>
 * </P>
 */
package eu.cactosfp7.datacollection.adaptor.sigarng;

import java.util.Timer;

import org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.log4j.Logger;

public class SystemMetricsNG extends AbstractAdaptor {
  static Logger log = Logger.getLogger(SystemMetricsNG.class);
  private long period = 60 * 1000;
  private SigarRunnerNG runner;
  private Timer timer;
  
  @Override
  public String parseArgs(String args) {
    int spOffset = args.indexOf(' ');
    log.info("args:"+args+" spOffset:"+spOffset);
    if (spOffset > 0) {
      try {
        period = Integer.parseInt(args.substring(0, spOffset));
        period = period * 1000;
        log.info("args:"+args+" period:"+period);
      } catch (NumberFormatException e) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("SystemMetrics: sample interval ");
        buffer.append(args.substring(0, spOffset));
        buffer.append(" can't be parsed.");
        log.warn(buffer.toString());
      }
    }    
    return args;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    if(timer == null) {
      timer = new Timer();
      runner = new SigarRunnerNG(dest, SystemMetricsNG.this);
    }
    timer.scheduleAtFixedRate(runner, 0, period);
    
  }

  @Override
  public String getCurrentStatus() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(type);
    buffer.append(" ");
    buffer.append(period/1000);
    return buffer.toString();
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    timer.cancel();
    return 0;
  }
  
  public long get_period(){
	  return period;
  }

}
