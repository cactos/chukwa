package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingSpecification.MappingKey;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.KeyRecordPair;
import static eu.cactosfp7.datacollection.chukwa.writer.hbase.row.CactosGenericRowKeyGenerator.TABLE_SEPARATOR;

public abstract class AbstractSingleValueRowGenerator implements IndexableRowKeyGenerator {

    protected static Logger log = Logger.getLogger(AbstractSingleValueRowGenerator.class);

    private final int myIndex;
    private final Map<MappingKey, ChukwaRecord> records = new HashMap<MappingKey, ChukwaRecord>();
    private final String rowKeyValue;

    protected AbstractSingleValueRowGenerator(String _rowKeyValue, int index) {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be null");
        }
        rowKeyValue = _rowKeyValue;
        myIndex = index;
    }

    protected AbstractSingleValueRowGenerator(String _rowKeyValue) {
        rowKeyValue = _rowKeyValue;
        myIndex = -1;
    }

    private void checkForConsistency() {
        String table = null;

        for (MappingKey m : records.keySet()) {
            String tab = m.getTable();
            if (table == null) {
                table = tab;
            } else {
                if (!table.equals(tab)) {
                    throw new IllegalStateException("table names do not match. this should not happen.");
                }
            }
        }
    }

    @Override
    public final void prepare() {
        records.clear();
    }

    private synchronized ChukwaRecord getChukwaRecord(MappingKey mappingKey) {
        ChukwaRecord record = records.get(mappingKey);
        if (record == null) {
            record = new ChukwaRecord();
            records.put(mappingKey, record);
        }
        return record;
    }

    @Override
    public final boolean hasIndex() {
        return myIndex > -1;
    }

    /**
     * @return the index if available
     * @throws IllegalStateException when hasIndex() would return false
     */
    @Override
    public final int getIndex() throws IllegalStateException {
        if (myIndex > -1) {
            return myIndex;
        }
        throw new IllegalStateException("no index available. cannot be returned");
    }

    @Override
    public final void bufferSingletonValue(MappingKey mappingKey, String columQuantifier, Object value) {
        ChukwaRecord record = getChukwaRecord(mappingKey);
        record.add(columQuantifier, value.toString());
    }

    @Override
    public final void bufferTupleValue(MappingKey mappingKey, String columQuantifier, int iterationCount, Object value) {
        ChukwaRecord record = getChukwaRecord(mappingKey);
        record.add(columQuantifier + "." + iterationCount, value.toString());
    }

    @Override
    public final void bufferAggregationValue(MappingKey mappingKey, String columQuantifier, Object value) {
        ChukwaRecord record = getChukwaRecord(mappingKey);
        record.add(columQuantifier, value.toString());
    }

    @Override
    public void bufferEmptyValue(MappingKey mappingKey) {
      ChukwaRecord record = getChukwaRecord(mappingKey);
//      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public final void collect(List<KeyRecordPair> results, Map<String, AccountingReadSpecification> elements) {
        //Commented to allow at the same mapping different tables :)
//        checkForConsistency();
        String key = generateKey(elements);
        createRecordKeySets(results, key);
    }

    protected abstract String generateKey(Map<String, AccountingReadSpecification> elements);

    private void createRecordKeySets(List<KeyRecordPair> results, String key) {

        for (MappingKey m : records.keySet()) {
            ChukwaRecord record = records.get(m);
            KeyRecordPair pair = new KeyRecordPair(record);
            pair.key.setReduceType(m.getColumn());
            pair.key.setKey(m.getTable() + TABLE_SEPARATOR + key);
            results.add(pair);
        }
    }

    protected final Object getRowKeyIndex() {
        return rowKeyValue;
    }
}
