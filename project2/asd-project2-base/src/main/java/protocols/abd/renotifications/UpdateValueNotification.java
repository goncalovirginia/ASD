package protocols.abd.renotifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class UpdateValueNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 521;

	private final byte[] key;
	private final int opSeq;
	private final byte[] value;

	public UpdateValueNotification(int opSeq, byte[] key, byte[] value) {
		super(NOTIFICATION_ID);
		this.opSeq = opSeq;
		this.key = key;
		this.value = value;
	}

	public int getOpSeq() {
		return opSeq;
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "UpdateValueNotification{" +
				"key=" + key +
				'}';
	}
}
