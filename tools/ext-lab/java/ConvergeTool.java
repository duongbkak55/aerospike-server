// P3b bin convergence test steps with the official Aerospike Java client. Each
// run does one step against namespace 'test' and prints "RESULT <value>".
//
//   ConvergeTool <port> put <set> <key-ix> <bin> <value>   write one bin (update)
//   ConvergeTool <port> delete-bin <set> <key-ix> <bin>    delete one bin
//   ConvergeTool <port> delete <set> <key-ix>              delete the record
//   ConvergeTool <port> get <set> <key-ix> <bin>           bin value, '-' if none,
//                                                          '!' if no record
//   ConvergeTool <port> random <set> <n> <ops> <bins> <seed>
//                       random puts, bin deletes, record deletes and increments
//                       on keys 0..n-1 and bins b0..b<bins-1>
//   ConvergeTool <port> digest <set> <n>   hash of keys 0..n-1 with all bins
//                                          (prints the dump too with DUMP=1)

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.security.MessageDigest;
import java.util.Random;
import java.util.TreeMap;

public class ConvergeTool {
	static final String NS = "test";

	public static void main(String[] args) throws Exception {
		int port = Integer.parseInt(args[0]);
		String step = args[1];
		String set = args[2];

		ClientPolicy cp = new ClientPolicy();
		cp.timeout = 5000;

		WritePolicy wp = new WritePolicy();
		wp.durableDelete = true;

		String result;

		try (AerospikeClient c = new AerospikeClient(cp, new Host("127.0.0.1", port))) {
			switch (step) {
			case "put":
				c.put(wp, key(set, Integer.parseInt(args[3])),
						new Bin(args[4], Long.parseLong(args[5])));
				result = "ok";
				break;
			case "delete-bin":
				c.put(wp, key(set, Integer.parseInt(args[3])), Bin.asNull(args[4]));
				result = "ok";
				break;
			case "delete":
				result = String.valueOf(c.delete(wp, key(set, Integer.parseInt(args[3]))));
				break;
			case "get": {
				Record r = c.get(null, key(set, Integer.parseInt(args[3])));
				Object v = r == null ? null : r.bins.get(args[4]);

				result = r == null ? "!" : v == null ? "-" : v.toString();
				break;
			}
			case "random": {
				int n = Integer.parseInt(args[3]);
				int ops = Integer.parseInt(args[4]);
				int bins = Integer.parseInt(args[5]);
				Random rnd = new Random(Long.parseLong(args[6]));

				for (int i = 0; i < ops; i++) {
					Key k = key(set, rnd.nextInt(n));
					String bin = "b" + rnd.nextInt(bins);
					int action = rnd.nextInt(100);

					try {
						if (action < 65) {
							c.put(wp, k, new Bin(bin, rnd.nextInt(1000000)));
						}
						else if (action < 80) {
							c.put(wp, k, Bin.asNull(bin));
						}
						else if (action < 90) {
							c.delete(wp, k);
						}
						else {
							c.add(wp, k, new Bin(bin, 1));
						}
					}
					catch (AerospikeException e) {
						// Deleting a bin of a missing record finds nothing.
						if (e.getResultCode() != ResultCode.KEY_NOT_FOUND_ERROR) {
							throw e;
						}
					}
				}

				result = String.valueOf(ops);
				break;
			}
			case "digest": {
				int n = Integer.parseInt(args[3]);
				StringBuilder sb = new StringBuilder();
				int live = 0;

				for (int i = 0; i < n; i++) {
					Record r = c.get(null, key(set, i));

					sb.append(i).append(':');

					if (r == null) {
						sb.append('-');
					}
					else {
						live++;
						sb.append(new TreeMap<>(r.bins));
					}

					sb.append('\n');
				}

				if ("1".equals(System.getenv("DUMP"))) {
					System.out.print(sb);
				}

				byte[] h = MessageDigest.getInstance("SHA-256").digest(
						sb.toString().getBytes("UTF-8"));
				StringBuilder hex = new StringBuilder();

				for (int i = 0; i < 8; i++) {
					hex.append(String.format("%02x", h[i]));
				}

				result = hex + "/" + live;
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
