package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.KeyRecordPair;

public final class ParallelCollector {

    private static Logger log = Logger.getLogger(ParallelCollector.class);

    private final List<MapperCollection> mappings = new ArrayList<MapperCollection>();
    private final Map<String, AccountingReadSpecification> allElements = new HashMap<String, AccountingReadSpecification>();

    private final String source;
    private volatile long timestamp = -1;

    public ParallelCollector(String _source) {
        source = _source;
    }

    public void setTimestamp(long _timestamp) {
        timestamp = _timestamp;
    }

    private void addContext() {
        Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(contextSpec);
        AbstractProcessorConstants.addSingleValue(AbstractProcessorConstants.TIMESTAMP, timestamp, elements);
        AbstractProcessorConstants.addSingleValue(AbstractProcessorConstants.SOURCE, source, elements);
        addData(elements);
    }

    public ParallelCollector addData(Map<String, AccountingReadSpecification> elements) {
        // just to stress the logging //
        for (String key : elements.keySet()) {
            AccountingReadSpecification spec = elements.get(key);
            spec = allElements.put(key, spec);
            if (spec != null) {
                throw new IllegalStateException("collection the same key multiple times: " + key);
            }
        }
        return this;
    }

    public synchronized ParallelCollector addMappings(MapperCollection mapping) {
        mappings.add(mapping);
        return this;
    }

    List<KeyRecordPair> harvest() {
        addContext();
        List<KeyRecordPair> result = new ArrayList<KeyRecordPair>();
        for (MapperCollection mapping : mappings) {
            // mapping.compareKeys(allElements.keySet());
            List<KeyRecordPair> tmp = mapping.collectData(allElements);
            result.addAll(tmp);
        }
        for (KeyRecordPair pair : result) {
            pair.value.setTime(timestamp);
        }
        return result;
    }

    long getTimestamp() {
        return timestamp;
    }

    public String getSource() {
        return source;
    }

    private static final Map<String, SourceReadSpecification> contextSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(
                new SourceReadSpecification[]{
                    new SourceReadSpecification(AbstractProcessorConstants.TIMESTAMP, long.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(AbstractProcessorConstants.SOURCE, String.class, ComputeType.IS_SINGLETON),}, contextSpec);
    }
}
