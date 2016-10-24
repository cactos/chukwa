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
package eu.cactosfp7.datacollection.chukwa.writer.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.hbase.client.Put;

import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.CactosGenericRowKeyGenerator;
import org.apache.hadoop.hbase.client.Delete;

public final class OutputCollectorNG implements
        org.apache.hadoop.mapred.OutputCollector<ChukwaRecordKey, ChukwaRecord> {

    private final Map<String, List<Put>> buffers = new HashMap<String, List<Put>>();
    private final Map<String, List<Delete>> deleteBuffers = new HashMap<String, List<Delete>>();

    private synchronized List<Put> getListForTable(String table, boolean createAndAdd) {
        List<Put> puts = buffers.get(table);
        if (puts == null && createAndAdd) {
            puts = new ArrayList<Put>();
            buffers.put(table, puts);
        }
        return puts;
    }

    private synchronized List<Delete> getDeleteListForTable(String table, boolean createAndAdd) {
        List<Delete> deletes = deleteBuffers.get(table);
        if (deletes == null && createAndAdd) {
            deletes = new ArrayList<Delete>();
            deleteBuffers.put(table, deletes);
        }
        return deletes;
    }

    public OutputCollectorNG() {
        // empty constructor //
    }

    @Override
    public synchronized void collect(ChukwaRecordKey key, ChukwaRecord value) throws IOException {
        CactosGenericRowKeyGenerator gen = CactosGenericRowKeyGenerator.fromString(key.getKey());
        String rowKeyString = gen.toRowKeyString();
        String reduceType = key.getReduceType();
        byte[] rowKey = gen.toRowKey();
        byte[] cf = key.getReduceType().getBytes();

//        Delete delete = buildDeleteCommand(rowKey, cf, value);
        Put kv = buildPutCommand(rowKey, cf, value);
//        addDeleteToTable(gen.getTableName(), delete);
        addPutToTable(gen.getTableName(), kv);
    }

    private void addDeleteToTable(String tableName, Delete delete) {
        List<Delete> deletes = getDeleteListForTable(tableName, true);
        deletes.add(delete);
    }

    private void addPutToTable(String tableName, Put kv) {
        List<Put> puts = getListForTable(tableName, true);
        puts.add(kv);
    }

    private Put buildPutCommand(byte[] rowKey, byte[] columnFamily, ChukwaRecord value) {
        long now = value.getTime();
        Put kv = new Put(rowKey);
        for (String field : value.getFields()) {
        	String toWrite = value.getValue(field);
            //kv.add(columnFamily, field.getBytes(), now, toWrite.getBytes());
            kv.add(columnFamily, field.getBytes(), toWrite.getBytes());
        }
        return kv;
    }

    private Delete buildDeleteCommand(byte[] rowKey, byte[] cf, ChukwaRecord value) {
        long now = value.getTime() - 1000L;
        Delete delete = new Delete(rowKey);
        delete.deleteFamily(cf, now);
        return delete;
    }

    public List<Put> getKeyValues(String table) {
        List<Put> puts = getListForTable(table, false);
        if (puts != null) {
            return puts;
        }
        return Collections.emptyList();
    }

    public List<Delete> getDeleteKeyValues(String table) {
        List<Delete> deletes = getDeleteListForTable(table, false);
        if (deletes != null) {
            return deletes;
        }
        return Collections.emptyList();
    }

}
