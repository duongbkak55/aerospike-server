// P2a tombstone test steps with the official Aerospike Java client. Each run
// does one step against namespace 'test' and prints "RESULT <n>".
//
//   TombstoneTool <port> write <set> <n> <value>    put keys <set>-0..n-1
//   TombstoneTool <port> delete <set> <n> <durable|plain>
//                                                   n = deletes that found a record
//   TombstoneTool <port> batch-delete <set> <n> <durable|plain>
//                                                   n = records deleted
//   TombstoneTool <port> count <set> <n>            n = keys that exist (gets)
//   TombstoneTool <port> scan <set>                 n = records a scan returns
//   TombstoneTool <port> generation <set> <key-ix>  n = record generation, -1 if none

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.util.concurrent.atomic.AtomicLong;

public class TombstoneTool {
	static final String NS = "test";

	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		String step = args[1];
		String set = args[2];

		ClientPolicy cp = new ClientPolicy();
		cp.timeout = 5000;

		long result;

		try (AerospikeClient c = new AerospikeClient(cp, new Host("127.0.0.1", port))) {
			switch (step) {
			case "write": {
				int n = Integer.parseInt(args[3]);
				int value = Integer.parseInt(args[4]);

				for (int i = 0; i < n; i++) {
					c.put(null, key(set, i), new Bin("v", value));
				}

				result = n;
				break;
			}
			case "delete": {
				int n = Integer.parseInt(args[3]);
				WritePolicy wp = new WritePolicy();

				wp.durableDelete = args[4].equals("durable");
				result = 0;

				for (int i = 0; i < n; i++) {
					if (c.delete(wp, key(set, i))) {
						result++;
					}
				}

				break;
			}
			case "batch-delete": {
				int n = Integer.parseInt(args[3]);
				BatchDeletePolicy dp = new BatchDeletePolicy();
				Key[] keys = new Key[n];

				dp.durableDelete = args[4].equals("durable");

				for (int i = 0; i < n; i++) {
					keys[i] = key(set, i);
				}

				BatchResults br = c.delete(null, dp, keys);
				result = 0;

				for (int i = 0; i < n; i++) {
					if (br.records[i].resultCode == 0) {
						result++;
					}
				}

				break;
			}
			case "count": {
				int n = Integer.parseInt(args[3]);
				result = 0;

				for (int i = 0; i < n; i++) {
					if (c.get(null, key(set, i)) != null) {
						result++;
					}
				}

				break;
			}
			case "scan": {
				AtomicLong count = new AtomicLong();

				c.scanAll(null, NS, set, (k, r) -> count.incrementAndGet());
				result = count.get();
				break;
			}
			case "generation": {
				Record r = c.getHeader(null, key(set, Integer.parseInt(args[3])));

				result = r == null ? -1 : r.generation;
				break;
			}
			default:
				throw new IllegalArgumentException("unknown step " + step);
			}
		}

		System.out.println("RESULT " + result);
	}

	static Key key(String set, int i) {
		return new Key(NS, set, set + "-" + i);
	}
}
