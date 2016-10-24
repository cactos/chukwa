package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.util.ArrayList; 
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import org.apache.log4j.Logger;

public final class AccountingReadSpecification {
	private final SourceReadSpecification mapping;
	private int iterationCount;
	private final List<Object> values = new ArrayList<Object>();
        private static Logger log = Logger.getLogger(AccountingReadSpecification.class);
	
	AccountingReadSpecification(SourceReadSpecification _mapping) {
		mapping = _mapping;
		iterationCount = 0;
	}

	public static Map<String, AccountingReadSpecification> fromReadWritemap( Map<String, SourceReadSpecification> map) {
		Map<String, AccountingReadSpecification> retval = new HashMap<String, AccountingReadSpecification>();
		for(String key : map.keySet()) {
			retval.put(key, new AccountingReadSpecification(map.get(key)));
		}
		return retval;
	}
	
	private double getValueAsDouble(Object value) {
		Class<?> clazz = value.getClass();
		if(clazz == Long.class) {return ((Long) value).doubleValue(); }
		if(clazz == Integer.class) {return ((Integer) value).doubleValue(); }
		if(clazz == Double.class) {return ((Double) value).doubleValue(); }
		if(clazz == Float.class) {return ((Float) value).doubleValue(); }
		if(clazz == Short.class) {return ((Short) value).doubleValue(); }
                try {
                if(clazz == String.class) {return (Double.parseDouble((String)value)); }
                } catch (NumberFormatException e) {
                    log.error("", e);
                }
		
                
		
		throw new IllegalArgumentException("unsupported Type: " + clazz);
	}

	public void addIteration(int iteration, Object value) {
		assert iterationCount == iteration : "iterations do not match";
		iterationCount++;
		values.add(value);
	}
	
	public boolean hasAggregation() {
		switch(mapping.comp){
		case SUM:
		case AVG:
			return true;
		case ONLY_PARTS:
		case IS_SINGLETON:
		default: 
			return false;
		}
	}
	
	public Object getAggragtion() {
		switch(mapping.comp){
		case SUM:
			return sum();
		case AVG:
			return sum() / iterationCount;
		case ONLY_PARTS:
			throw new IllegalStateException("I should never be here");
		case IS_SINGLETON:
			throw new IllegalStateException("I should never be here");
		default: 
			throw new IllegalStateException("missing case in swich construc?: " + mapping.comp);
		}
	}
	
	private final void writeAggregation(ChukwaRecord record, String writeKeyBasis) {
		switch(mapping.comp){
		case SUM:
			double d = sum();
			record.add(writeKeyBasis, Double.toString(d));
			break;
		case AVG:
			d = sum() / iterationCount;
			record.add(writeKeyBasis, Double.toString(d));
			break;
		case ONLY_PARTS:
			return;
		case IS_SINGLETON:
			throw new IllegalStateException("I should never be here");
		default: 
			throw new IllegalStateException("missing case in swich construc?: " + mapping.comp);
		}
	}
	public void addAllToRecord(ChukwaRecord record, String writeKeyBasis) {
		final int a = values.size();
		for(int i = 0; i < a; i++) {
			String writeKey = getKeyName(i, writeKeyBasis);
			record.add(writeKey, values.get(i).toString());
		}
		writeAggregation(record, writeKeyBasis);
	}
	
	private final double sum() {
		final int a = values.size();
		double overallCount = 0.0;
		for(int i = 0; i < a; i++) {
			Object value = values.get(i);
			double toAdd = getValueAsDouble(value);
			overallCount = overallCount + toAdd;
		}
		return overallCount;
	}
	
	private final String getKeyName(int iteration, String writeKey) {
		if(iteration == 0 && mapping.comp == ComputeType.IS_SINGLETON) {
			return writeKey;
		} /* else if(iteration == 0 && mapping.comp != ComputeType.IS_SINGLETON) {
			throw new IllegalStateException("invalid combination");
		} */
		return writeKey + "." + iteration;
	}

	public List<Object> getCollection() {
		return Collections.unmodifiableList(values);
	}

	public boolean isSingleton() {
		return mapping.comp == ComputeType.IS_SINGLETON;
	}
}