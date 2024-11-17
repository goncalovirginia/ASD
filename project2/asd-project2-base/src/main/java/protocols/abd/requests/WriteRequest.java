package protocols.abd.requests;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class WriteRequest extends ProtoRequest {

	public static final short REQUEST_ID = 511;

	private final UUID opId;
	private final byte[] key;
	private final byte[] data;

	public WriteRequest(UUID opId, byte[] key, byte[] data) {
		super(REQUEST_ID);
		this.opId = opId;
		this.key = key;
		this.data = data;
	}

	public UUID getOpId() {
		return opId;
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public String toString() {
		return "WriteRequest{" +
				"opId=" + opId +
				", operation=" + Hex.encodeHexString(key) +
				'}';
	}
}
