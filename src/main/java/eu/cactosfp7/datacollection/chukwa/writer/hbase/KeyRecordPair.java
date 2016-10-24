package eu.cactosfp7.datacollection.chukwa.writer.hbase;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingSpecification.MappingValue;

public final class KeyRecordPair implements MappingValue {
	
	public final ChukwaRecordKey key = new ChukwaRecordKey();
	public final ChukwaRecord value;
	
	//private KeyRecodPair(ChukwaRecordKey _key, ChukwaRecord _value) {
	//	key = _key;
	//	value = _value;
	//}
	public KeyRecordPair() {
		value = new ChukwaRecord();
	}
	
	public KeyRecordPair(ChukwaRecord _record) {
		value = _record;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if(o == this) return true;
		if(!(o instanceof KeyRecordPair)) return false;
		KeyRecordPair that = (KeyRecordPair) o;
		
		boolean a = (this.key == that.key) || (this.key != null && this.key.equals(that.key));
		boolean b = (this.value == that.value) || (this.value != null && this.value.equals(that.value));
		return a && b;
	}
}