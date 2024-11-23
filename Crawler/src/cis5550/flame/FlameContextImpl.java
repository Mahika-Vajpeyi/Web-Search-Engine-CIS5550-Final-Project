package cis5550.flame;

import java.io.Serializable;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext, Serializable{

	public String outputString = null;
	
	public String jarName = null;
	private String coordinatorArg;
	private KVSClient kvsClient;
	
	public FlameContextImpl(String jarName) {
		// TODO Auto-generated constructor stub
		this.jarName = jarName;
	}

	@Override
	public KVSClient getKVS() {
		kvsClient = new KVSClient(coordinatorArg);
		return kvsClient;
	}
	
	public void setCoordinator(String coorArg)
	{
		coordinatorArg = coorArg;
	}

	@Override
	public void output(String s) {
		if (outputString == null) {
			outputString = s;
		} else {
			outputString += s; 
		}
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		String tableName = UUID.randomUUID().toString();
		int i = 0;
		for (String s : list) {
			Coordinator.kvs.put(tableName, Hasher.hash(Integer.toString(i)), "value", s);
			i++;
		}
		FlameRDDImpl rddImpl = new FlameRDDImpl(tableName);
		return rddImpl;
	}
	
	static String invokeOperation (String operationName, String reqType, String inputTable, String zeroElement, byte[] lambda, double prob, FlamePairRDDImpl other) {
		String newTableName = UUID.randomUUID().toString();
		Partitioner partitioner = new Partitioner();
//		add all workers, then partition
		try {
			int numWorkers = Coordinator.kvs.numWorkers();
			for (int i = 0; i < numWorkers - 1; i++) {
				partitioner.addKVSWorker(Coordinator.kvs.getWorkerAddress(i), Coordinator.kvs.getWorkerID(i), Coordinator.kvs.getWorkerID(i+1));
			}
			partitioner.addKVSWorker(Coordinator.kvs.getWorkerAddress(numWorkers-1), null, Coordinator.kvs.getWorkerID(0));
			partitioner.addKVSWorker(Coordinator.kvs.getWorkerAddress(numWorkers-1), Coordinator.kvs.getWorkerID(0), null);
			
			Vector<String> flameWorkers = Coordinator.getWorkers();
			int numFlameWorkers = flameWorkers.size();
			for (int i = 0; i < numFlameWorkers; i++) {
				partitioner.addFlameWorker(flameWorkers.get(i));
			}
			
//			Assigns KVS key range for flame workers to handle
			Vector<Partition> partitions = partitioner.assignPartitions();
			Thread threads[] = new Thread[partitions.size()];
			String threadsResponse[] = new String[partitions.size()];
			int threadsResponseCodes[] = new int[partitions.size()];
			int i = 0;
			for (Partition p : partitions) {
				final int z = i;
				threads[i] = new Thread() {
					public void run() {
						try {
							String url = "http://"+p.assignedFlameWorker+operationName;
							String urlParams = "?input="+inputTable+"&output="+newTableName+
											   "&coordinator="+Coordinator.kvs.getCoordinator();
							if (p.fromKey == null) {
								urlParams = urlParams + "&high="+p.toKeyExclusive;
							} else if (p.toKeyExclusive == null) {
								urlParams = urlParams + "&low="+p.fromKey;
							} else {
								urlParams = urlParams + "&low="+p.fromKey+"&high="+p.toKeyExclusive;
							}
							if (zeroElement != null) {
								urlParams = urlParams + "&zeroElement="+URLEncoder.encode(zeroElement, "UTF-8");
							}
							if (prob != -1) {
								urlParams = urlParams + "&prob="+Double.toString(prob);
							}
							if (other != null) {
								urlParams = urlParams + "&other="+other.tableName;
							}
							Response r = HTTP.doRequest(reqType, url+urlParams, lambda);
							threadsResponse[z] = new String(r.body());
							threadsResponseCodes[z] = r.statusCode();
						} catch (Exception e) {
					        e.printStackTrace();
						}
					}
				};
				threads[i].start();
				i++;
			}
			for (int j=0; j<threads.length; j++) {
				try {
					threads[j].join();
		        } catch (InterruptedException ie) {
		        	ie.printStackTrace();
		        }
			}
			for (int k = 0; k < threadsResponse.length; k++) {
				if (!threadsResponse[k].equals("OK") || threadsResponseCodes[k] != 200) {
					return "FAIL";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return newTableName;
	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String newTableName = FlameContextImpl.invokeOperation("/rdd/fromTable", "POST", tableName, null, Serializer.objectToByteArray(lambda), -1, null);
		FlameRDDImpl rddImpl = new FlameRDDImpl(newTableName);
		return rddImpl;
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		
	}
	

}
