package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;

public final class AbstractProcessorConstants {

    public static final String SYSTEM_METRICS = "Metrics";
    public static final String INDEX_TABLE = "Index";
    public static final String CN_TABLE = "CNSnapshot";
    public static final String ST_TABLE = "StorageSnapshot";
    public static final String CN_HISTORY_TABLE = "CNHistory";
    public static final String ST_HISTORY_TABLE = "StorageHistory";
    public static final String VM_TABLE = "VMSnapshot";
    public static final String VM_HISTORY_TABLE = "VMHistory";
    public static final String VM_APP_HISTORY_TABLE = "VMAppHistory";

    static final int DATE_START_INDEX = 0;
    static final int DATE_END_INDEX = 23;

    public static final String TIMESTAMP = "Timestamp";
    public static final String SOURCE = "Source";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    static Logger log = Logger.getLogger(AbstractProcessorNG.class);

    public static int compareArrays(String[] save, String[] toCompare, int compareOffset) {
        for (int i = 0; i < save.length; i++) {
            String parser = save[i];
            String that = toCompare[i + compareOffset];
            if (!parser.equals(that)) {
                return i;
            }
        }
        return -1;
    }

    public static String skipStartSequence(String recordEntry, String startString) {
        int start = DATE_END_INDEX;
        int idx = 0;
        for (int i = 0; i < 2; i++) {
            idx = recordEntry.indexOf(' ', start + 1);
            start++;
        }
        idx = recordEntry.indexOf(startString, idx + 1);

        return recordEntry.substring(idx);
    }

    public final static void setTimestamp(String recordEntry, ParallelCollector collector) {
        final String dStr = recordEntry.substring(DATE_START_INDEX, DATE_END_INDEX);
        long timestamp = 0L;
        try {
            Date d = sdf.parse(dStr);
            timestamp = d.getTime();
        } catch (java.text.ParseException e) {
            log.error("cannot parse date: " + dStr + "; using current timestamp.");
            timestamp = System.currentTimeMillis();
        }
        collector.setTimestamp(timestamp);
    }

    public static <T extends Specification> void addToMap(T[] inL, Map<String, T> out) {
        for (T in : inL) {
            out.put(in.getSourceKey(), in);
        }
    }

    public static void addAndCount(String key, int iteration, Map<String, ?> values, Map<String, AccountingReadSpecification> map) {
        AccountingReadSpecification counter = map.get(key);
        if (counter == null) {
            log.error("no mapping found for key " + key);
            return;
        }

        // this can never be null; //
        Object value = values.get(key);
        counter.addIteration(iteration, value);
    }

    public static void addSingleValueAtIndex(String key, int index, Object value, Map<String, AccountingReadSpecification> map) {
        AbstractProcessorConstants.addAndCount(key, index, Collections.singletonMap(key, value), map);
    }

    public static void addSingleValue(String key, Object value, Map<String, AccountingReadSpecification> map) {
        AbstractProcessorConstants.addAndCount(key, 0, Collections.singletonMap(key, value), map);
    }

    private AbstractProcessorConstants() {
        // no instances of this class
    }

    //private final HashMap<String, HBaseMappingSpecification> allOtherMapping = new HashMap<String, HBaseMappingSpecification>();
    public static final Map<String, SourceReadSpecification> allOtherReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        addToMap(
                new SourceReadSpecification[]{
                    new SourceReadSpecification(TIMESTAMP, long.class, ComputeType.IS_SINGLETON),}, allOtherReadSpec);
    }

    /*public static MapperCollection<HBaseMappingSpecification> getFilledAllOtherMapping(String _columFamily, RowKeyGenerator generator) {
     final MapperCollection<HBaseMappingSpecification> tmp = new MapperCollection<HBaseMappingSpecification>();
     return tmp.addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TIMESTAMP, INDEX_TABLE, _columFamily, generator));
     }*/
}