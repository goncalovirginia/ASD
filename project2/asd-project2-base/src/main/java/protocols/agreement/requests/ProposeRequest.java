package protocols.agreement.requests;

import org.apache.commons.codec.binary.Hex;
import protocols.statemachine.messages.LeaderOrderMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class ProposeRequest extends ProtoRequest {

	public static final short REQUEST_ID = 101;

	private final int instance;
	private final UUID opId;
	private final byte[] operation;

	public ProposeRequest(int instance, UUID opId, byte[] operation) {
		super(REQUEST_ID);
		this.instance = instance;
		this.opId = opId;
		this.operation = operation;
	}

	public ProposeRequest(LeaderOrderMessage msg) {
		super(REQUEST_ID);
		this.instance = msg.getInstance();
		this.opId = msg.getOpId();
		this.operation = msg.getOp();
	}

	public int getInstance() {
		return instance;
	}

	public byte[] getOperation() {
		return operation;
	}

	public UUID getOpId() {
		return opId;
	}

	@Override
	public String toString() {
		return "ProposeRequest{" +
				"instance=" + instance +
				", opId=" + opId +
				", operation=" + Hex.encodeHexString(operation) +
				'}';
	}
}
