package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.KeyRecordPair;

public final class MapperCollection<T extends MappingSpecification> {
	
	private static Logger log = Logger.getLogger(AbstractProcessorNG.class);
	
	private final HashMap<String, List<T>> mappings = new HashMap<String, List<T>>();
	private final Set<MappingCollector> collectors = new HashSet<MappingCollector>();
	
	public MapperCollection() {
		
	}
	
	/** 
	 * registers a mapping that uses the default mapper 
	 */
	public synchronized MapperCollection<T> addMapping(T toAdd) {
		final String key = toAdd.getSourceKey();
		List<T> container = mappings.get(key);
		if(container == null) {
			container = new ArrayList<T>();
			mappings.put(key, container);
		}
		container.add(toAdd);
		collectors.add(toAdd.getCollector());
		
		return this;
	}

	/** @Deprecated */
	public void compareKeys(Set<String> elementsKeySet) {
		Set<String> keys1 = new HashSet<String>(elementsKeySet);
		Set<String> keys2 = new HashSet<String>(elementsKeySet);
		Set<String> mapping = collectMappingKeys();
		
		Set<String> mappings1 = new HashSet<String>(mapping);
		Set<String> mappings2 = new HashSet<String>(mapping);
		
		mappings1.removeAll(keys1);
		if(mappings1.size() != 0) {
			log.warn("there are mappings without a matching element: " + mappings1);
		}
		keys2.removeAll(mappings2);
		if(keys2.size() != 0) {
			log.warn("there are elements left that will not be matched to the database: " + keys2);
		}
	}
	
	private Set<String> collectMappingKeys() {
		Set<String> keys = new HashSet<String>();
		for(String key : mappings.keySet()) {
			log.error("this method has not been implemented");
		}
		return keys;
	}
	
	private void collectDataFromMappings(Map<String, AccountingReadSpecification> elements) {
//		compareKeys(elements.keySet());
		
        for(MappingCollector mc : collectors){
        	mc.prepare();
        }
	
		for(String key : mappings.keySet()) {
			List<T> list = mappings.get(key);
			for(T t : list) { 
				t.collect(elements); 
			}
		}
	}
	
	List<KeyRecordPair> collectData(Map<String, AccountingReadSpecification> elements) {
		collectDataFromMappings(elements);
		List<KeyRecordPair> results = new ArrayList<KeyRecordPair>();
		for(MappingCollector mc : collectors){mc.collect(results, elements);}
		return results;
	}
}
