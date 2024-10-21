package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FoundSuccessorMessage extends ProtoMessage {

	public static final short MSG_ID = 522;

	private final UUID mid;
	private final Host originalSenderHost, senderHost, successorHost;

	private final BigInteger key, senderPeerID, successorPeerID;

	@Override
	public String toString() {
		return "FoundSuccessorMessage{" +
				"mid=" + mid +
				'}';
	}

	//original sender can be any1
	public FoundSuccessorMessage(UUID mid, Host originalSenderHost, Host senderHost, Host successorHost, BigInteger key, BigInteger senderPeerID, BigInteger successorPeerID) {
		super(MSG_ID);
		this.mid = mid;
		this.originalSenderHost = originalSenderHost;
		this.senderHost = senderHost;
		this.successorHost = successorHost;
		this.key = key;
		this.senderPeerID = senderPeerID;
		this.successorPeerID = successorPeerID;
	}

	public FoundSuccessorMessage(FindSuccessorMessage findSuccessorMessage, ChordNode thisNode, ChordNode successorNode) {
		super(MSG_ID);
		this.mid = findSuccessorMessage.getMid();
		this.originalSenderHost = findSuccessorMessage.getOriginalSender();
		this.senderHost = thisNode.getHost();
		this.successorHost = successorNode.getHost();
		this.key = findSuccessorMessage.getKey();
		this.senderPeerID = thisNode.getPeerID();
		this.successorPeerID = successorNode.getPeerID();

	}

	public Host getOriginalSenderHost() {
		return originalSenderHost;
	}

	public Host getSenderHost() {
		return senderHost;
	}

	public Host getSuccessorHost() {
		return successorHost;
	}

	public UUID getMid() {
		return mid;
	}

	public BigInteger getKey() {
		return key;
	}

	public BigInteger getSenderPeerID() {
		return senderPeerID;
	}

	public BigInteger getSuccessorPeerID() {
		return successorPeerID;
	}

	public static ISerializer<FoundSuccessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(FoundSuccessorMessage foundSuccessorMessage, ByteBuf out) throws IOException {
			// Serialize UUID (mid)
			out.writeLong(foundSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(foundSuccessorMessage.mid.getLeastSignificantBits());

			// Serialize Hosts (originalSenderHost, senderHost, successorHost)
			Host.serializer.serialize(foundSuccessorMessage.originalSenderHost, out);
			Host.serializer.serialize(foundSuccessorMessage.senderHost, out);
			Host.serializer.serialize(foundSuccessorMessage.successorHost, out);


			// Serialize BigInteger fields (key, senderPeerID, successorPeerID)
			byte[] keyByteArray = foundSuccessorMessage.key.toByteArray();
			out.writeInt(keyByteArray.length);
			out.writeBytes(keyByteArray);

			byte[] senderPeerIDByteArray = foundSuccessorMessage.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);

			byte[] successorPeerIDByteArray = foundSuccessorMessage.successorPeerID.toByteArray();
			out.writeInt(successorPeerIDByteArray.length);
			out.writeBytes(successorPeerIDByteArray);

		}

		@Override
		public FoundSuccessorMessage deserialize(ByteBuf in) throws IOException {
			// Deserialize UUID (mid)
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);

			// Deserialize Hosts (originalSenderHost, senderHost, successorHost)
			Host originalSender = Host.serializer.deserialize(in);
			Host sender = Host.serializer.deserialize(in);
			Host successor = Host.serializer.deserialize(in);

			// Deserialize BigInteger fields (key, senderPeerID, successorPeerID)
			int keySize = in.readInt();
			byte[] keyByteArray = new byte[keySize];
			in.readBytes(keyByteArray);

			int senderPeerIDSize = in.readInt();
			byte[] senderPeerIDByteArray = new byte[senderPeerIDSize];
			in.readBytes(senderPeerIDByteArray);

			int successorPeerIDSize = in.readInt();
			byte[] successorPeerIDByteArray = new byte[successorPeerIDSize];
			in.readBytes(successorPeerIDByteArray);

			// Return the deserialized message
			return new FoundSuccessorMessage(mid, originalSender, sender, successor,
					new BigInteger(1, keyByteArray), new BigInteger(1, senderPeerIDByteArray), new BigInteger(1, successorPeerIDByteArray));
		}
	};
}
