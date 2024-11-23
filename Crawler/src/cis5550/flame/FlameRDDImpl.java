package cis5550.flame;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD{
	public String tableName = null;

	public FlameRDDImpl(String tableName) {
		// TODO Auto-generated constructor stub
		this.tableName = tableName;
	}

	@Override
	public List<String> collect() throws Exception {
		List<String> elements = new ArrayList<String>();
		Iterator<Row> tableScan = Coordinator.kvs.scan(tableName);
		while (tableScan.hasNext()) {
			Row next = tableScan.next();
			elements.add(next.get("value"));
		}
		return elements;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/flatMap", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/mapToPair", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlamePairRDDImpl rddPairImpl = new FlamePairRDDImpl(newTableName);
		return rddPairImpl;
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		List<String> tableOne = new ArrayList<>(new HashSet<>(collect()));
		List<String> tableTwo = new ArrayList<>(new HashSet<>(r.collect()));
		List<String> common = new ArrayList<>(tableOne);
		common.retainAll(tableTwo);
		KVSClient kvs = Coordinator.kvs;
		String newTableName = UUID.randomUUID().toString();
		int i = 0;
		for (String s : common) {
			kvs.put(newTableName, Hasher.hash(Integer.toString(i)), "value", s);
			i++;
		}
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/sampling", "POST", tableName, null, null, f, null);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
//		String newTableName = FlameContextImpl.invokeOperation("/rdd/groupBy", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1);
//		FlamePairRDDImpl rddPairImpl = new FlamePairRDDImpl(newTableName);
//		return rddPairImpl;
		return null;
	}

	@Override
	public int count() throws Exception {
		// TODO Auto-generated method stub
		KVSClient kvs = Coordinator.kvs;
		return kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		KVSClient kvs = Coordinator.kvs;
		kvs.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	@Override
	public FlameRDD distinct() throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/distinct", "POST", tableName, null, null, -1, null);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public void destroy() throws Exception {
		KVSClient kvs = Coordinator.kvs;
		kvs.delete(tableName);
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		// TODO Auto-generated method stub
		KVSClient kvs = Coordinator.kvs;
		Vector<String> elements = new Vector<String>();
		Iterator<Row> tableScan = kvs.scan(tableName);
		int i = 0;
		while (tableScan.hasNext() && i < num) {
			Row next = tableScan.next();
			elements.add(next.get("value"));
			i++;
		}
		return elements;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/fold", "POST", tableName, zeroElement, Serializer.objectToByteArray(lambda), -1, null);
//		String[] accumulators = FlameContextImpl.invokeOperationFold("/rdd/fold", "POST", tableName, zeroElement, Serializer.objectToByteArray(lambda), -1, null);
		String acc = zeroElement;
		KVSClient kvs = Coordinator.kvs;
		Iterator<Row> tableScan = kvs.scan(newTableName);
		while(tableScan.hasNext()) {
			Row row = tableScan.next();
			acc = lambda.op(acc, row.get("value"));
		}
        return acc;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/flatMapToPair", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlamePairRDDImpl rddImpl = new FlamePairRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("/rdd/filter", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
