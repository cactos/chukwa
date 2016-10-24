package eu.cactosfp7.datacollection.chukwa.writer.hbase;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingSpecification.MappingKey;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;

public final class HBaseMappingSpecification implements MappingSpecification<HBaseMappingSpecification.HBaseMappingKey> {

    static Logger log = Logger.getLogger(HBaseMappingSpecification.class);

    private final String readKey;
    private final String tableName;
    private final String columnFamily;
    private final String columQuantifier;
    private final RowKeyGenerator generator;

    private HBaseMappingSpecification(String _readKey, String _tableName, String _columnFamily, String _columQuantifier, RowKeyGenerator _generator) {
        readKey = _readKey;
        tableName = _tableName;
        columnFamily = _columnFamily;
        columQuantifier = _columQuantifier;
        generator = _generator;
    }

    @Override
    public HBaseMappingKey getMappingKey() {
        return new HBaseMappingKey(tableName, columnFamily);
    }

    /**
     * writes an element stored at a 'readKey' in 'SourceReadSpecification' to
     * an HBase cell 'columnFamily:readKey' at table 'tableName'
     *
     * @param readKey
     * @param tableName
     * @param columnFamily
     * @return
     */
    public static HBaseMappingSpecification readKeyAsQuantifierMapping(String readKey, String tableName, String columnFamily, RowKeyGenerator _generator) {
        return new HBaseMappingSpecification(readKey, tableName, columnFamily, readKey, _generator);
    }

    /**
     * writes an element stored at a 'readKey' in 'SourceReadSpecification' to
     * an HBase cell 'columnFamily:quantifier' at table 'tableName'
     *
     * @param readKey
     * @param tableName
     * @param columnFamily
     * @return
     */
    public static HBaseMappingSpecification arbitraryQuantifierMapping(String readKey, String tableName, String columnFamily, String quantifier, RowKeyGenerator _generator) {
        return new HBaseMappingSpecification(readKey, tableName, columnFamily, quantifier, _generator);
    }

    /**
     * is used to identify the 'key' in the data source. hence defines for which
     * tuple this mapping is valid. the same key can be applied in multiple
     * mappings. In that case, however, for these mappings one of rowKey
     * construction, table, or column family has to be different. Alternatively,
     * a different mapping key has to be established.
     */
    @Override
    public String getSourceKey() {
        return readKey;
    }

    @Override
    public String toString() {
        return readKey + " => " + "{ " + tableName + "/" + columnFamily + ":" + columQuantifier + "}";
    }

    private synchronized KeyRecordPair getContent(HBaseMappingKey key, Map<HBaseMappingKey, KeyRecordPair> perTableRecords) {
        KeyRecordPair o = perTableRecords.get(key);
        if (o == null) {
            o = new KeyRecordPair();
            perTableRecords.put(key, o);
        }
        return o;
    }

    public static class HBaseMappingKey implements MappingKey {

        final String table;
        final String columnF;

        private HBaseMappingKey(String _table, String _columnF) {
            table = _table;
            columnF = _columnF;
        }

        @Override
        public String getTable() {
            return table;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + ((columnF == null) ? 0 : columnF.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof HBaseMappingKey)) {
                return false;
            }
            HBaseMappingKey that = (HBaseMappingKey) o;

            boolean a = (this.table == that.table) || (this.table != null && this.table.equals(that.table));
            boolean b = (this.columnF == that.columnF) || (this.columnF != null && this.columnF.equals(that.columnF));
            return a && b;
        }

        @Override
        public String getColumn() {
            return columnF;
        }
        
        @Override
        public String toString() {
        	return "HBaseMappingKey: {" + table + "/" + columnF + "}";
        }
    }

    @Override
    public MappingCollector getCollector() {
        return generator;
    }

    @Override
    public void collect(Map<String, AccountingReadSpecification> elements) {
        AccountingReadSpecification accounting = elements.get(readKey);

        List<Object> collection = accounting.getCollection();
        if (accounting.isSingleton()) {
            assert collection.size() == 1 : "list length does not match";
            if (collection.size() == 0) {
                log.error("got no read values for key <" + readKey + "> and cannot perform mapping. Fix configuration!");
                return;
            }
            if (collection.size() > 1) {
                log.error("got multiple read read values for SINGLETON key <" + readKey + ">. Fix configuration!");
            }
            generator.bufferSingletonValue(getMappingKey(), columQuantifier, collection.get(0));
        } else {
            if (collection.size() == 0) {
                generator.bufferEmptyValue(getMappingKey());
            }

            for (int i = 0; i < collection.size(); i++) {
                Object value = collection.get(i);
                // => table name, column family, column qualifier, row key.
                generator.bufferTupleValue(getMappingKey(), columQuantifier, i, value);
            }
        }

        if (accounting.hasAggregation()) {
            Object agg = accounting.getAggragtion();
            generator.bufferAggregationValue(getMappingKey(), columQuantifier, agg);
        }
    }
}
