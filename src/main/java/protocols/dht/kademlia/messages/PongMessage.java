package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class PongMessage extends ProtoMessage {

	public static final short MSG_ID = 507;

	private final UUID messageID;
	private final Host sender;
	private final BigInteger key;

	@Override
	public String toString() {
		return "PongMessage{" +
				"mid=" + messageID +
				'}';
	}

	public PongMessage(UUID messageID, Host sender, BigInteger key) {
		super(MSG_ID);
		this.messageID = messageID;
		this.sender = sender;
		this.key = key;
	}

	public Host getSender() {
		return sender;
	}

	public UUID getMessageID() {
		return messageID;
	}

	public BigInteger getKey() {
		return key;
	}

	public static ISerializer<PongMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(PongMessage findNodeMessage, ByteBuf out) throws IOException {
			out.writeLong(findNodeMessage.messageID.getMostSignificantBits());
			out.writeLong(findNodeMessage.messageID.getLeastSignificantBits());
			Host.serializer.serialize(findNodeMessage.sender, out);
			byte[] keyByteArray = findNodeMessage.key.toByteArray();
			out.writeInt(keyByteArray.length);
			out.writeBytes(keyByteArray);
		}

		@Override
		public PongMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] keyByteArray = new byte[size];
			in.readBytes(keyByteArray);

			return new PongMessage(mid, sender, new BigInteger(keyByteArray));
		}
	};
}
