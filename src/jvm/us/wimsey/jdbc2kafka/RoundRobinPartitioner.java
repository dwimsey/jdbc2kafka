/**
 * Copyright 2015 Bandwidth, Inc - All rights reserved.
 * Author: David Wimsey <dwimsey@bandwidth.com>
 */
package us.wimsey.jdbc2kafka;
import org.apache.log4j.Logger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
public class RoundRobinPartitioner implements Partitioner {
    public static Logger LOG = Logger.getLogger(RoundRobinPartitioner.class);

    public RoundRobinPartitioner(VerifiableProperties props) {
 	}

    static int partition = 0;
    public int partition(Object key, int a_numPartitions) {
/*
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
*/
        return partition++ % a_numPartitions;
  }
 
}
