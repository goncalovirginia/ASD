package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.KademliaNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class KBucketMessage extends ProtoMessage {

	public static final short MSG_ID = 503;

	private final UUID messageID;
	private final Host sender;
	private final BigInteger key;
	private final KademliaNode[] kBucket;

	@Override
	public String toString() {
		return "ReturnKBucketMessage{" +
				"mid=" + messageID +
				'}';
	}

	public KBucketMessage(UUID messageID, Host sender, BigInteger key, KademliaNode[] kBucket) {
		super(MSG_ID);
		this.messageID = messageID;
		this.sender = sender;
		this.key = key;
		this.kBucket = kBucket;
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

	public static ISerializer<KBucketMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(KBucketMessage findNodeMessage, ByteBuf out) throws IOException {
			out.writeLong(findNodeMessage.messageID.getMostSignificantBits());
			out.writeLong(findNodeMessage.messageID.getLeastSignificantBits());
			Host.serializer.serialize(findNodeMessage.sender, out);
			byte[] keyByteArray = findNodeMessage.key.toByteArray();
			out.writeInt(keyByteArray.length);
			out.writeBytes(keyByteArray);
		}

		@Override
		public KBucketMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] keyByteArray = new byte[size];
			in.readBytes(keyByteArray);

			return new KBucketMessage(mid, sender, new BigInteger(keyByteArray), new KademliaNode[1]);
		}
	};
}
