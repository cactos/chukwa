package eu.cactosfp7.datacollection.chukwa.demux.mapper;

public class SourceReadSpecification implements Specification {
	private final String readKey;
	//final String writeKey;
	private final Class<?> readType;
	//private final Class<?> writeType;
	final ComputeType comp;
	
	@Override
	public String getSourceKey(){ return readKey; }
	
	public SourceReadSpecification(String readK, Class<?> readClazz, ComputeType _comp) {
		readKey = readK; readType = readClazz; comp = _comp;
	}

	public static enum ComputeType {
		SUM,
		AVG,
		ONLY_PARTS,
		IS_SINGLETON,
		GROUP_BY_PARTS,
		;
	}
}