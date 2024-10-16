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

	private final BigInteger senderPeerID;

	private final int fingerIndex;
	private final BigInteger fingerStart;

	@Override
	public String toString() {
		return "FixFingerMessage{" +
				"mid=" + mid +
				'}';
	}

	public FixFingerMessage(UUID mid, Host sender, BigInteger senderPeerID, int fingerIndex, BigInteger fingerStart ) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.senderPeerID = senderPeerID;
		this.fingerIndex = fingerIndex;
		this.fingerStart = fingerStart;
	}

	public FixFingerMessage(UUID mid, ChordNode thisNode, int fingerIndex, BigInteger fingerStart) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = thisNode.getHost();
		this.senderPeerID = thisNode.getPeerID();
		this.fingerIndex = fingerIndex;
		this.fingerStart = fingerStart;
	}

	public Host getSender() {
		return sender;
	}

	public UUID getMid() {
		return mid;
	}

	public BigInteger getSenderPeerID() {
		return senderPeerID;
	}

	public int getFingerIndex() {
		return fingerIndex;
	}

	public BigInteger getFingerStart() {
		return fingerStart;
	}

	public static ISerializer<FixFingerMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(FixFingerMessage findSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(findSuccessorMessage.sender, out);
			byte[] senderPeerIDByteArray = findSuccessorMessage.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);
			out.writeInt(findSuccessorMessage.fingerIndex);
			byte[] fingerStartByteArray = findSuccessorMessage.fingerStart.toByteArray();
			out.writeInt(fingerStartByteArray.length);
			out.writeBytes(fingerStartByteArray);
		}

		@Override
		public FixFingerMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] senderPeerIDByteArray = new byte[size];
			in.readBytes(senderPeerIDByteArray);
			int fingerIndex = in.readInt();
			size = in.readInt();
			byte[] fingerStartByteArray = new byte[size];
			in.readBytes(fingerStartByteArray);

			return new FixFingerMessage(mid, sender, new BigInteger(senderPeerIDByteArray), fingerIndex, new BigInteger(fingerStartByteArray));
		}
	};
}
