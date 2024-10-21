package utils;

import com.google.common.hash.Hashing;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HashProducer {
	private final ByteBuffer append;
	private final int off;
	private final int size;
	private final byte[] selfAddr;

	public HashProducer(Host self) {
		selfAddr = self.getAddress().getAddress();
		size = selfAddr.length + Integer.BYTES + Long.BYTES;
		append = ByteBuffer.allocate(size);
		append.put(selfAddr);
		append.putInt(self.getPort());
		this.off = append.arrayOffset();
	}


	public int hash(byte[] contents) {
		ByteBuffer buffer = ByteBuffer.allocate(contents.length + size);
		buffer.put(contents);
		append.putLong(off, System.currentTimeMillis());
		buffer.put(append);
		return Arrays.hashCode(buffer.array());
	}

	public int hash() {
		return Arrays.hashCode(selfAddr);
	}

	public static BigInteger toNumberFormat(byte[] peerID) {
		return new BigInteger(peerID);
	}

	public static byte[] hashValue(String value) {
		return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).asBytes();
	}

	public static byte[] hashValue(String value, int numBits) {
		if (numBits == 512) {
			return Hashing.sha512().hashString(value, StandardCharsets.UTF_8).asBytes();
		}
		return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).asBytes();
	}

	public static int randomInitializer(byte[] peerID) {
		return Arrays.hashCode(peerID);
	}
}
