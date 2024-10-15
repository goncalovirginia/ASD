package protocols.dht.chord_concurrent.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord_concurrent.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FixFingerMessage extends ProtoMessage {

	public static final short MSG_ID = 506;

	private final UUID mid;
	private final Host sender;

	private final short toDeliver;
	private final BigInteger senderPeerID;

	private final int fingerIndex;
	private final BigInteger fingerStart;

	@Override
	public String toString() {
		return "ReturnPredecessorMessage{" +
				"mid=" + mid +
				'}';
	}

	public FixFingerMessage(UUID mid, short toDeliver, Host sender, BigInteger senderPeerID, int fingerIndex, BigInteger fingerStart) {
		super(MSG_ID);
		this.mid = mid;
		this.toDeliver = toDeliver;
		this.sender = sender;
		this.senderPeerID = senderPeerID;
		this.fingerIndex = fingerIndex;
		this.fingerStart = fingerStart;
	}

	public FixFingerMessage(UUID mid, short toDeliver, ChordNode thisNode, int fingerIndex, BigInteger fingerStart) {
		super(MSG_ID);
		this.mid = mid;
		this.toDeliver = toDeliver;
		this.sender = thisNode.getHost();
		this.senderPeerID = thisNode.getPeerID();
		this.fingerIndex = fingerIndex;
		this.fingerStart = fingerStart;
	}

	public Host getSender() {
		return sender;
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

	public static ISerializer<FixFingerMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(FixFingerMessage findSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
			out.writeShort(findSuccessorMessage.toDeliver);
			Host.serializer.serialize(findSuccessorMessage.sender, out);
			byte[] senderPeerIDByteArray = findSuccessorMessage.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);
		}

		@Override
		public FixFingerMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			short toDeliver = in.readShort();
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] senderPeerIDByteArray = new byte[size];
			in.readBytes(senderPeerIDByteArray);

			return new FixFingerMessage(mid, toDeliver, sender, new BigInteger(senderPeerIDByteArray));
		}
	};
}
