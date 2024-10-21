package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class ReturnPredecessorMessage extends ProtoMessage {

	public static final short MSG_ID = 504;

	private final UUID mid;
	private final Host predecessor, sender, successor;
	private final BigInteger predecessorPeerID, senderPeerID, successorPeerID;

	@Override
	public String toString() {
		return "ReturnPredecessorMessage{" +
				"mid=" + mid +
				'}';
	}

	public ReturnPredecessorMessage(UUID mid, Host predecessor, Host sender, Host successor, BigInteger predecessorPeerID, BigInteger senderPeerID, BigInteger successorPeerID) {
		super(MSG_ID);
		this.mid = mid;
		this.predecessor = predecessor;
		this.sender = sender;
		this.successor = successor;
		this.predecessorPeerID = predecessorPeerID;
		this.senderPeerID = senderPeerID;
		this.successorPeerID = successorPeerID;
	}

	public ReturnPredecessorMessage(UUID mid, ChordNode predecessorNode, ChordNode thisNode, ChordNode successorNode) {
		super(MSG_ID);
		this.mid = mid;
		this.predecessor = predecessorNode.getHost();
		this.sender = thisNode.getHost();
		this.successor = successorNode.getHost();
		this.predecessorPeerID = predecessorNode.getPeerID();
		this.senderPeerID = thisNode.getPeerID();
		this.successorPeerID = successorNode.getPeerID();
	}

	public UUID getMid() {
		return mid;
	}

	public Host getPredecessor() {
		return predecessor;
	}

	public Host getSender() {
		return sender;
	}

	public Host getSuccessor() {
		return successor;
	}

	public BigInteger getPredecessorPeerID() {
		return predecessorPeerID;
	}

	public BigInteger getSenderPeerID() {
		return senderPeerID;
	}

	public BigInteger getSuccessorPeerID() {
		return successorPeerID;
	}

	public static ISerializer<ReturnPredecessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(ReturnPredecessorMessage message, ByteBuf out) throws IOException {
			out.writeLong(message.mid.getMostSignificantBits());
			out.writeLong(message.mid.getLeastSignificantBits());
			Host.serializer.serialize(message.predecessor, out);
			Host.serializer.serialize(message.sender, out);
			Host.serializer.serialize(message.successor, out);

			byte[] predecessorPeerIDByteArray = message.predecessorPeerID.toByteArray();
			out.writeInt(predecessorPeerIDByteArray.length);
			out.writeBytes(predecessorPeerIDByteArray);

			byte[] senderPeerIDByteArray = message.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);

			byte[] successorPeerIDByteArray = message.successorPeerID.toByteArray();
			out.writeInt(successorPeerIDByteArray.length);
			out.writeBytes(successorPeerIDByteArray);
		}

		@Override
		public ReturnPredecessorMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host predecessor = Host.serializer.deserialize(in);
			Host sender = Host.serializer.deserialize(in);
			Host successor = Host.serializer.deserialize(in);

			int predecessorPeerIDSize = in.readInt();
			byte[] predecessorPeerIDByteArray = new byte[predecessorPeerIDSize];
			in.readBytes(predecessorPeerIDByteArray);
			BigInteger predecessorPeerID = new BigInteger(1, predecessorPeerIDByteArray);

			int senderPeerIDSize = in.readInt();
			byte[] senderPeerIDByteArray = new byte[senderPeerIDSize];
			in.readBytes(senderPeerIDByteArray);
			BigInteger senderPeerID = new BigInteger(1, senderPeerIDByteArray);

			int successorPeerIDSize = in.readInt();
			byte[] successorPeerIDByteArray = new byte[successorPeerIDSize];
			in.readBytes(successorPeerIDByteArray);
			BigInteger successorPeerID = new BigInteger(1, successorPeerIDByteArray);

			return new ReturnPredecessorMessage(mid, predecessor, sender, successor, predecessorPeerID, senderPeerID, successorPeerID);
		}
	};
}