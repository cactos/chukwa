package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MappingSpecification.MappingKey;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.KeyRecordPair;

public final class SplittingRowKeyGenerator<T extends IndexableRowKeyGenerator> implements RowKeyGenerator {

    private final List<Buffer> buffered = new ArrayList<Buffer>();
    private final Map<Integer, T> divisions = new HashMap<Integer, T>();

    private final Class<T> clazz;
    private final Object[] params;

    public SplittingRowKeyGenerator(Class<T> _clazz, Object... _params) {
        clazz = _clazz;
        params = _params;
    }

    private Constructor findConstructor() {
        Constructor[] cs = clazz.getConstructors();
        OUTER:
        for (Constructor c : cs) {
            Class[] classes = c.getParameterTypes();
            if (classes.length != params.length + 1) {
                continue;
            }
            for (int i = 0; i < params.length; i++) {
                if (!classes[i].isAssignableFrom(params[i].getClass())) {
                    continue OUTER;
                }
            }
            if (!(classes[params.length].isAssignableFrom(int.class)
                    || classes[params.length].isAssignableFrom(Integer.class))) {
                continue;
            }
            // here we are; we found something //
            return c;
        }
        return null;
    }

    private synchronized final T getDivision(int id) {
        T t = divisions.get(id);
        if (t == null) {
            try {
                Constructor c = findConstructor();
                if (c == null) {
                    throw new IllegalArgumentException("could not find constructor");
                }
                Object[] para = new Object[params.length + 1];
                System.arraycopy(params, 0, para, 0, params.length);
                para[params.length] = id;
                t = (T) c.newInstance(para);
            } catch (InstantiationException e) {
                throw new IllegalStateException("cannot create row generator: " + e.getMessage());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("cannot create row generator: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                throw new IllegalStateException("cannot create row generator: " + e.getMessage());
            }
            divisions.put(id, t);
        }
        return t;
    }

    @Override
    public void prepare() {
        divisions.clear();
        buffered.clear();
    }

    @Override
    public void collect(List<KeyRecordPair> results, Map<String, AccountingReadSpecification> elements) {
        registerSingleValues();
        for (Integer i : divisions.keySet()) {
            divisions.get(i).collect(results, elements);
        }
    }

    private void registerSingleValues() {
        for (Buffer b : buffered) {
            for (Integer i : divisions.keySet()) {
                T t = divisions.get(i);
                t.bufferSingletonValue(b.mappingKey, b.columQuantifier, b.object);
            }
        }
    }

    @Override
    public void bufferSingletonValue(MappingKey mappingKey, String columQuantifier, Object object) {
        Buffer b = new Buffer(mappingKey, columQuantifier, object);
        buffered.add(b);
    }

    @Override
    public void bufferTupleValue(MappingKey mappingKey, String columQuantifier, int i, Object value) {
        T t = getDivision(i);
        t.bufferSingletonValue(mappingKey, columQuantifier, value);
    }

    @Override
    public void bufferAggregationValue(MappingKey mappingKey, String columQuantifier, Object agg) {
        throw new UnsupportedOperationException("cannot add aggregation values to SplittingRowKeyGenerator");
    }

    @Override
    public void bufferEmptyValue(MappingKey mappingKey) {
        
    }

    private static class Buffer {

        final MappingKey mappingKey;
        final String columQuantifier;
        final Object object;

        Buffer(MappingKey _mappingKey, String _columQuantifier, Object _object) {
            mappingKey = _mappingKey;
            columQuantifier = _columQuantifier;
            object = _object;
        }
    }
}
