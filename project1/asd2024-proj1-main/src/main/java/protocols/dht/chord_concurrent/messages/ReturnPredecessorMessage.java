package protocols.dht.chord_concurrent.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord_concurrent.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class ReturnPredecessorMessage extends ProtoMessage {

	public static final short MSG_ID = 504;

	private final UUID mid;
	private final Host sender, predecessor;

	private final short toDeliver;
	private final BigInteger senderPeerID, predecessorPeerID;

	@Override
	public String toString() {
		return "ReturnPredecessorMessage{" +
				"mid=" + mid +
				'}';
	}

	public ReturnPredecessorMessage(UUID mid, short toDeliver, Host sender, Host predecessor, BigInteger senderPeerID, BigInteger predecessorPeerID) {
		super(MSG_ID);
		this.mid = mid;
		this.toDeliver = toDeliver;
		this.sender = sender;
		this.predecessor = predecessor;
		this.senderPeerID = senderPeerID;
		this.predecessorPeerID = predecessorPeerID;
	}

	public ReturnPredecessorMessage(UUID mid, short toDeliver, ChordNode thisNode, ChordNode predecessorNode) {
		super(MSG_ID);
		this.mid = mid;
		this.toDeliver = toDeliver;
		this.sender = thisNode.getHost();
		this.predecessor = predecessorNode.getHost();
		this.senderPeerID = thisNode.getPeerID();
		this.predecessorPeerID = predecessorNode.getPeerID();
	}


	public Host getSender() {
		return sender;
	}

	public Host getPredecessor() {
		return predecessor;
	}

	public short getToDeliver() {
		return toDeliver;
	}

	public UUID getMid() {
		return mid;
	}

	public BigInteger getSenderPeerID() {
		return senderPeerID;
	}

	public BigInteger getPredecessorPeerID() {
		return predecessorPeerID;
	}

	public static ISerializer<ReturnPredecessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(ReturnPredecessorMessage findSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
			out.writeShort(findSuccessorMessage.toDeliver);
			Host.serializer.serialize(findSuccessorMessage.sender, out);
			byte[] senderPeerIDByteArray = findSuccessorMessage.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);
		}

		@Override
		public ReturnPredecessorMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			short toDeliver = in.readShort();
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] senderPeerIDByteArray = new byte[size];
			in.readBytes(senderPeerIDByteArray);

			return new ReturnPredecessorMessage(mid, toDeliver, sender, new BigInteger(senderPeerIDByteArray));
		}
	};
}
