package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.util.List;
import java.util.Map;

import eu.cactosfp7.datacollection.chukwa.writer.hbase.KeyRecordPair;

public interface MappingCollector {

	void prepare();

	void collect(List<KeyRecordPair> results, Map<String, AccountingReadSpecification> elements);

}
