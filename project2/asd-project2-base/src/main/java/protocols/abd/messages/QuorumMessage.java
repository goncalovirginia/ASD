package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class QuorumMessage extends ProtoMessage {

	public final static short MSG_ID = 501;

	private final int opSeq;
	private final String key;
	private final boolean isRead;

	public QuorumMessage(int opSeq, String key, boolean r) {
		super(MSG_ID);
		this.opSeq = opSeq;
		this.key = key;
		this.isRead = r;
	}

	public int getOpSeq() {
		return opSeq;
	}

	public String getKey() {
		return key;
	}

	public boolean isRead() {
		return isRead;
	}

	@Override
	public String toString() {
		String start = "";
		if (isRead)
			start = "ReadMessage{";
		else
			start = "ReadTagMessage{";

		return start +
				"opSeq=" + opSeq +
				", key=" + key +
				'}';
	}

	public static ISerializer<QuorumMessage> serializer = new ISerializer<QuorumMessage>() {
		@Override
		public void serialize(QuorumMessage msg, ByteBuf out) {
			out.writeInt(msg.opSeq);
			byte[] keyBytes = msg.key.getBytes();
			out.writeInt(keyBytes.length);
			out.writeBytes(keyBytes);
			out.writeBoolean(msg.isRead);
		}

		@Override
		public QuorumMessage deserialize(ByteBuf in) {
			int instance = in.readInt();
			int keyLength = in.readInt();
			byte[] keyBytes = new byte[keyLength];
			in.readBytes(keyBytes);

			String key = new String(keyBytes);
			boolean reading = in.readBoolean();

			return new QuorumMessage(instance, key, reading);
		}
	};

}
