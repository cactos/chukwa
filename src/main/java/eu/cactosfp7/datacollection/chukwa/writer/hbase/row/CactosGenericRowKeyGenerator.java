package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;

public final class CactosGenericRowKeyGenerator {

	public final static String TABLE_SEPARATOR = "/";
	public final static String TIMESTAMP_SEPARATOR = "-";
	
	private String key;
	//private String timestamp;
	private String table;
	
	private CactosGenericRowKeyGenerator(String _key, String _table) {
		key = _key;
		table = _table;
		// timestamp = _timestamp;
	}
	
	@Override
	public String toString() {
		return table + "/" + key;
	}

	void asKey(ChukwaRecordKey key) {
		key.setKey(toString());
	}
	
	public static CactosGenericRowKeyGenerator fromString(String keyString) {
		String[] vals = keyString.split("/");
		if(vals.length != 2) throw new IllegalArgumentException("wrong key format: " + keyString);
		// FIXME: add further validity checks //
		return new CactosGenericRowKeyGenerator(vals[1], vals[0]);
	}

	public String toRowKeyString() {
		return key;
	}
	
	public byte[] toRowKey() {
		//String s = timestamp + "-" + dataSource;
		return key.getBytes();
	}

	public String getTableName() {
		return table;
	}

	public static boolean isValidKey(String key) {
		try { fromString(key); return true; }
		catch(Throwable t) { return false; }
	}
}
