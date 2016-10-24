package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.TIMESTAMP;
import static eu.cactosfp7.datacollection.chukwa.writer.hbase.row.CactosGenericRowKeyGenerator.TIMESTAMP_SEPARATOR;

import java.util.Map;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;

public final class TimestampedRowGenerator extends AbstractSingleValueRowGenerator {
	//IndexableRowKeyGenerator
	
	public TimestampedRowGenerator(String _rowKeyValue, int index) {
		super(_rowKeyValue, index);
	}
	
	public TimestampedRowGenerator(String _rowKeyValue) {
		super(_rowKeyValue);
	}

	@Override
	protected String generateKey(Map<String, AccountingReadSpecification> elements) {
		//FIXME: add checks for null //
		AccountingReadSpecification time = elements.get(TIMESTAMP);
		AccountingReadSpecification source = elements.get(getRowKeyIndex());
		assert time.isSingleton() : "timestamp is not singleton value";
		
		final String source_string = RowKeyGeneratorHelper.getRowKeyValueFromKeyIndexAndMapIndex(source, this);
		final String timestamp_string = time.getCollection().get(0).toString();
		
		return source_string + TIMESTAMP_SEPARATOR + timestamp_string ;
	}
	
}
