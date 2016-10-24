package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingSpecification.MappingKey;

public interface RowKeyGenerator extends MappingCollector {

    void bufferSingletonValue(MappingKey mappingKey, String columQuantifier, Object object);
    void bufferTupleValue(MappingKey mappingKey, String columQuantifier, int i, Object value);
    void bufferAggregationValue(MappingKey mappingKey, String columQuantifier, Object agg); 
    void bufferEmptyValue(MappingKey mappingKey);
}
