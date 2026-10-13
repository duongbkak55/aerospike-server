// P1a data-operation RBAC checks with the official Aerospike Java client.
//
// Build & run (any jar containing aerospike-client, e.g. kvbench.jar):
//   javac -cp <client.jar> -d /tmp/rbac DataRbacTest.java
//   java -cp <client.jar>:/tmp/rbac DataRbacTest [port]
//
// Expects a security-enabled cluster where p1a_data_test.sh has created the
// users below.

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;

public class DataRbacTest {
	static final int ROLE_VIOLATION = 81;
	static final String NS = "test";

	static int failures = 0;

	interface Op {
		void run() throws Exception;
	}

	static void check(boolean cond, String what) {
		System.out.println((cond ? "PASS: " : "FAIL: ") + what);

		if (! cond) {
			failures++;
		}
	}

	static void expectOk(String what, Op op) {
		try {
			op.run();
			check(true, what);
		}
		catch (Exception e) {
			check(false, what + " (" + e.getMessage() + ")");
		}
	}

	static void expectCode(int code, String what, Op op) {
		try {
			op.run();
			check(false, what + " (no error, want " + code + ")");
		}
		catch (AerospikeException e) {
			check(e.getResultCode() == code,
					what + " (got " + e.getResultCode() + ", want " + code + ")");
		}
		catch (Exception e) {
			check(false, what + " (" + e + ")");
		}
	}

	static AerospikeClient connect(int port, String user, String password) {
		ClientPolicy policy = new ClientPolicy();

		policy.user = user;
		policy.password = password;
		policy.timeout = 3000;

		return new AerospikeClient(policy, new Host("127.0.0.1", port));
	}

	static Key key(String set, String k) {
		return new Key(NS, set, k);
	}

	public static void main(String[] args) {
		int port = args.length > 0 ? Integer.parseInt(args[0]) : 3000;

		// Connecting.
		// The client wraps seed login failures - look for the cause.
		try {
			connect(port, "rw", "wrong").close();
			check(false, "wrong password can't connect");
		}
		catch (AerospikeException e) {
			check(String.valueOf(e.getMessage()).contains("65") ||
					String.valueOf(e.getMessage()).contains("credential"),
					"wrong password can't connect (" + e.getMessage() + ")");
		}

		try {
			AerospikeClient c = connect(port, null, null);
			boolean ok = c.isConnected();

			c.close();
			check(! ok, "no credentials can't use the cluster");
		}
		catch (AerospikeException e) {
			check(true, "no credentials can't use the cluster (" +
					e.getResultCode() + ")");
		}

		try (AerospikeClient rw = connect(port, "rw", "rw-pw");
				AerospikeClient ro = connect(port, "ro", "ro-pw");
				AerospikeClient wo = connect(port, "wo", "wo-pw");
				AerospikeClient scoped = connect(port, "scoped", "scoped-pw")) {

			// read-write.
			expectOk("read-write can write", () -> {
				for (String set : new String[] { "s1", "s2", "s3" }) {
					for (int i = 0; i < 20; i++) {
						rw.put(null, key(set, "k" + i), new Bin("v", i));
					}
				}
			});
			expectOk("read-write can read", () -> {
				Record r = rw.get(null, key("s1", "k1"));
				check(r != null && r.getInt("v") == 1, "read-write reads its write");
			});
			expectOk("read-write can delete",
					() -> rw.delete(null, key("s3", "k19")));
			expectOk("read-write can batch read", () -> {
				Record[] rs = rw.get(null, new Key[] { key("s1", "k1"),
						key("s2", "k2") });
				check(rs[0] != null && rs[1] != null, "batch returns records");
			});

			// Scans (queries without a filter).
			expectOk("read-write can scan a set", () -> {
				int[] n = { 0 };
				rw.scanAll(null, NS, "s1", (k, r) -> n[0]++);
				check(n[0] >= 20, "scan returns the set's records (" + n[0] + ")");
			});

			// read.
			expectOk("read can read", () -> ro.get(null, key("s1", "k2")));
			expectOk("read can scan",
					() -> ro.scanAll(null, NS, "s1", (k, r) -> {}));
			expectCode(ROLE_VIOLATION, "read can't write",
					() -> ro.put(null, key("s1", "x"), new Bin("v", 1)));
			expectCode(ROLE_VIOLATION, "read can't delete",
					() -> ro.delete(null, key("s1", "k2")));

			// write.
			expectOk("write can write",
					() -> wo.put(null, key("s1", "w"), new Bin("v", 1)));
			expectCode(ROLE_VIOLATION, "write can't read",
					() -> wo.get(null, key("s1", "w")));
			expectCode(ROLE_VIOLATION, "write can't scan",
					() -> wo.scanAll(null, NS, "s1", (k, r) -> {}));

			// Scoped: read on test.s1, read-write on test.s2.
			expectOk("scoped can read its read set",
					() -> scoped.get(null, key("s1", "k3")));
			expectCode(ROLE_VIOLATION, "scoped can't write its read set",
					() -> scoped.put(null, key("s1", "x"), new Bin("v", 1)));
			expectOk("scoped can write its read-write set",
					() -> scoped.put(null, key("s2", "x"), new Bin("v", 1)));
			expectCode(ROLE_VIOLATION, "scoped can't read another set",
					() -> scoped.get(null, key("s3", "k3")));
			expectOk("scoped can scan its read set",
					() -> scoped.scanAll(null, NS, "s1", (k, r) -> {}));
			expectCode(ROLE_VIOLATION, "scoped can't scan another set",
					() -> scoped.scanAll(null, NS, "s3", (k, r) -> {}));
			expectCode(ROLE_VIOLATION, "scoped can't read without a set",
					() -> scoped.get(null, new Key(NS, (String)null, "k3")));

			try {
				Record[] rs = scoped.get(null, new Key[] { key("s1", "k4"),
						key("s3", "k4") });
				check(rs[0] != null && rs[1] == null,
						"scoped batch read gets only permitted records");
			}
			catch (AerospikeException e) {
				check(e.getResultCode() == ROLE_VIOLATION,
						"scoped batch read is refused for the forbidden key (" +
						e.getResultCode() + ")");
			}
		}

		System.out.println(failures + " failure(s)");
		System.exit(failures == 0 ? 0 : 1);
	}
}
