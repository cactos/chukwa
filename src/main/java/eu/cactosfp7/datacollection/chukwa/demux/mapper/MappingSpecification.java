package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.util.Map;  

public interface MappingSpecification<T extends MappingSpecification.MappingKey> extends Specification {

	public interface MappingValue {
		
	}
	
	public interface MappingKey {

		String getTable();
		String getColumn();
		
	}
	
	T getMappingKey();
	MappingCollector getCollector();
	void collect(Map<String, AccountingReadSpecification> elements);

}
