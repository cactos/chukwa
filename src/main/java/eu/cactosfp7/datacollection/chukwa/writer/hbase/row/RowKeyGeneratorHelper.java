package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;

final class RowKeyGeneratorHelper {
	private static final Logger log = Logger.getLogger(RowKeyGeneratorHelper.class);
	
	static final String getRowKeyValueFromKeyIndexAndMapIndex(AccountingReadSpecification source,
			IndexableRowKeyGenerator generator) {
		if(source.isSingleton() && generator.hasIndex() ) {
			log.warn("generating key from a singleton value + " + source.getCollection().get(0) + "  + even though index + " + generator.getIndex() + " + was set.");
			return source.getCollection().get(0).toString();
		} 
		if(! source.isSingleton() && !generator.hasIndex() ) {
			log.warn("generating key from a non-singleton value even though no index was set."
					+ "using default value.");
			return source.getCollection().get(0).toString();
		}
		
		if(source.isSingleton() && !generator.hasIndex() ) {
			return source.getCollection().get(0).toString();
		}
		
		if(!source.isSingleton() && generator.hasIndex() ) {
			return source.getCollection().get(generator.getIndex()).toString();
		}
		
		throw new IllegalStateException("four if statements with two booleans should be enough.");
	}
	
	private RowKeyGeneratorHelper() {
		// no instances of this class //
	}
}
