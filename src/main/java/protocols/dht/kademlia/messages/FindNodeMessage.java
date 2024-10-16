package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.KademliaNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindNodeMessage extends ProtoMessage {

	public static final short MSG_ID = 501;

	private final UUID messageID;
	private final Host sender;
	private final BigInteger senderID, key;

	@Override
	public String toString() {
		return "FindNodeMessage{" +
				"mid=" + messageID +
				'}';
	}

	public FindNodeMessage(UUID messageID, KademliaNode thisNode, BigInteger key) {
		super(MSG_ID);
		this.messageID = messageID;
		this.sender = thisNode.getHost();
		this.senderID = thisNode.getPeerID();
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

	public static ISerializer<FindNodeMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(FindNodeMessage findNodeMessage, ByteBuf out) throws IOException {
			out.writeLong(findNodeMessage.messageID.getMostSignificantBits());
			out.writeLong(findNodeMessage.messageID.getLeastSignificantBits());
			Host.serializer.serialize(findNodeMessage.sender, out);
			byte[] senderIDByteArray = findNodeMessage.senderID.toByteArray();
			out.writeInt(senderIDByteArray.length);
			out.writeBytes(senderIDByteArray);
			byte[] keyByteArray = findNodeMessage.key.toByteArray();
			out.writeInt(keyByteArray.length);
			out.writeBytes(keyByteArray);
		}

		@Override
		public FindNodeMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] senderIDByteArray = new byte[size];
			in.readBytes(senderIDByteArray);
			size = in.readInt();
			byte[] keyByteArray = new byte[size];
			in.readBytes(keyByteArray);

			return new FindNodeMessage(mid, new KademliaNode(new BigInteger(senderIDByteArray), sender), new BigInteger(keyByteArray));
		}
	};
}
