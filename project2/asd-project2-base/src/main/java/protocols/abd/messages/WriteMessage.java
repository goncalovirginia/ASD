package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import protocols.abd.utils.Tag;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class WriteMessage extends ProtoMessage {

	public final static short MSG_ID = 503;

	private final int opSeq;
	private final String key;
	private final Tag tag;
	private final byte[] value;

	public WriteMessage(int opSeq, String key, Tag tag, byte[] val) {
		super(MSG_ID);
		this.opSeq = opSeq;
		this.key = key;
		this.tag = tag;
		this.value = val;
	}

	public int getOpId() {
		return opSeq;
	}

	public String getKey() {
		return key;
	}

	public Tag getTag() {
		return tag;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "WriteMessage{" +
				"opSeq=" + opSeq +
				", tag=" + tag +
				'}';
	}

	public static ISerializer<WriteMessage> serializer = new ISerializer<WriteMessage>() {
		@Override
		public void serialize(WriteMessage msg, ByteBuf out) {
			out.writeInt(msg.opSeq);

			byte[] keyBytes = msg.key.getBytes();
			out.writeInt(keyBytes.length);
			out.writeBytes(keyBytes);

			out.writeInt(msg.tag.getOpSeq());
			out.writeInt(msg.tag.getProcessId());

			out.writeInt(msg.value.length);
			out.writeBytes(msg.value);
		}

		@Override
		public WriteMessage deserialize(ByteBuf in) {
			int instance = in.readInt();

			byte[] keyBytes = new byte[in.readInt()];
			in.readBytes(keyBytes);
			String key = new String(keyBytes);

			int left = in.readInt();
			int right = in.readInt();
			Tag tag = new Tag(left, right);

			byte[] dt = new byte[in.readInt()];
			in.readBytes(dt);

			return new WriteMessage(instance, key, tag, dt);
		}
	};

}
