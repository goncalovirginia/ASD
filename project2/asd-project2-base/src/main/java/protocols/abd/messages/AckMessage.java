package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AckMessage extends ProtoMessage {

	public final static short MSG_ID = 504;

	private final int opSeq;
	private final String key;

	public AckMessage(int opSeq, String key) {
		super(MSG_ID);
		this.opSeq = opSeq;
		this.key = key;
	}

	public int getOpId() {
		return opSeq;
	}

	public String getKey() {
		return key;
	}

	@Override
	public String toString() {
		return "ACKMessage{" +
				"opSeq=" + opSeq +
				", key=" + key +
				'}';
	}

	public static ISerializer<AckMessage> serializer = new ISerializer<AckMessage>() {
		@Override
		public void serialize(AckMessage msg, ByteBuf out) {
			out.writeInt(msg.opSeq);

			byte[] keyBytes = msg.key.getBytes();
			out.writeInt(keyBytes.length);
			out.writeBytes(keyBytes);
		}

		@Override
		public AckMessage deserialize(ByteBuf in) {
			int instance = in.readInt();

			byte[] keyBytes = new byte[in.readInt()];
			in.readBytes(keyBytes);
			String key = new String(keyBytes);

			return new AckMessage(instance, key);
		}
	};

}
