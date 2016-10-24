/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.chukwa.datacollection.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 * @author bwpc
 */
public class MockTeeOutputCollector implements OutputCollector<ChukwaRecordKey, ChukwaRecord> {
    List<ChukwaRecord> chukwaRecords;
    List<ChukwaRecordKey> chukwaRecordKeys;

    public MockTeeOutputCollector() {
        chukwaRecords = new ArrayList<ChukwaRecord>();
        chukwaRecordKeys = new ArrayList<ChukwaRecordKey>();
    }

    @Override
    public void collect(ChukwaRecordKey key, ChukwaRecord value) throws IOException {
        chukwaRecordKeys.add(key);
        chukwaRecords.add(value);
    }

    public List<ChukwaRecordKey>  getChukwaRecordKeys() {
        return chukwaRecordKeys;
    }

    public List<ChukwaRecord> getChukwaRecords() {
        return chukwaRecords;
    }
}
