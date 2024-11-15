package protocols.statemachine.notifications;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.UUID;

public class ExecuteNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 202;

	private final UUID opId;
	private final byte[] operation;

	public ExecuteNotification(UUID opId, byte[] operation) {
		super(NOTIFICATION_ID);
		this.opId = opId;
		this.operation = operation;
	}

	public byte[] getOperation() {
		return operation;
	}

	public UUID getOpId() {
		return opId;
	}

	@Override
	public String toString() {
		return "ExecuteNotification{" +
				"opId=" + opId +
				", operation=" + Hex.encodeHexString(operation) +
				'}';
	}
}
