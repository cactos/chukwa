package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import java.util.Map;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;

public final class PlainValueRowGenerator extends AbstractSingleValueRowGenerator {

	public PlainValueRowGenerator(String _rowKeyValue, int index) {
		super(_rowKeyValue, index);
	}
	
	public PlainValueRowGenerator(String _rowKeyValue) {
		super(_rowKeyValue);
	}

	@Override
	protected String generateKey(Map<String, AccountingReadSpecification> elements) {
		//FIXME: add checks for null //
		AccountingReadSpecification source = elements.get(super.getRowKeyIndex());
		assert source.isSingleton() : "not singleton values";
		return RowKeyGeneratorHelper.getRowKeyValueFromKeyIndexAndMapIndex(source, this);
	}
}
