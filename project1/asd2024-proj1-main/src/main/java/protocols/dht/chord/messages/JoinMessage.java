package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class JoinMessage extends ProtoMessage {
	public static final short MSG_ID = 501;

	private final UUID mid;
	private final Host sender;

	private final short toDeliver;
	private final byte[] content;

	@Override
	public String toString() {
		return "JoinMessage{" +
				"mid=" + mid +
				'}';
	}

	public JoinMessage(UUID mid, Host sender, short toDeliver, byte[] content) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.toDeliver = toDeliver;
		this.content = content;
	}

	public Host getSender() {
		return sender;
	}

	public UUID getMid() {
		return mid;
	}

	public short getToDeliver() {
		return toDeliver;
	}

	public byte[] getContent() {
		return content;
	}

	public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(JoinMessage joinMessage, ByteBuf out) throws IOException {
			out.writeLong(joinMessage.mid.getMostSignificantBits());
			out.writeLong(joinMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(joinMessage.sender, out);
			out.writeShort(joinMessage.toDeliver);
			out.writeInt(joinMessage.content.length);
			if (joinMessage.content.length > 0) {
				out.writeBytes(joinMessage.content);
			}
		}

		@Override
		public JoinMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			short toDeliver = in.readShort();
			int size = in.readInt();
			byte[] content = new byte[size];
			if (size > 0)
				in.readBytes(content);

			return new JoinMessage(mid, sender, toDeliver, content);
		}
	};
}
