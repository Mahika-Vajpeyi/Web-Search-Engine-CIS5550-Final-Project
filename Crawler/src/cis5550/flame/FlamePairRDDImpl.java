package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD{
	public String tableName = null;

	public FlamePairRDDImpl(String tableName) {
		// TODO Auto-generated constructor stub
		this.tableName = tableName;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> elements = new ArrayList<FlamePair>();
		Iterator<Row> tableScan = Coordinator.kvs.scan(tableName);
		while (tableScan.hasNext()) {
			Row next = tableScan.next();
			String rowKey = next.key();
			Set<String> columns = next.columns();
			for (String c : columns) {
				String value = next.get(c);
				elements.add(new FlamePair(rowKey, value));
			}
		}
		return elements;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String newTableName = FlameContextImpl.invokeOperation("/rddPair/foldByKey", "POST", tableName, zeroElement, Serializer.objectToByteArray(lambda), -1, null);
		FlamePairRDDImpl rddPairImpl = new FlamePairRDDImpl(newTableName);
		return rddPairImpl;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.rename(tableName, tableNameArg);
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rddPair/flatMap", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public void destroy() throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.delete(tableName);
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rddPair/flatMapToPair", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlamePairRDDImpl rddImpl = new FlamePairRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rddPair/join", "POST", tableName, null, null, -1, (FlamePairRDDImpl) other);
		FlamePairRDDImpl rddImpl = new FlamePairRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
