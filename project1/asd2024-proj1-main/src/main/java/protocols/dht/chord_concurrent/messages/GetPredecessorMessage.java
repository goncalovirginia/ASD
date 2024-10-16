package protocols.dht.chord_concurrent.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord_concurrent.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class GetPredecessorMessage extends ProtoMessage {

	public static final short MSG_ID = 503;

	private final UUID mid;
	private final Host sender;

	private final BigInteger senderPeerID;

	@Override
	public String toString() {
		return "GetPredecessorMessage{" +
				"mid=" + mid +
				'}';
	}

	public GetPredecessorMessage(UUID mid, Host sender, BigInteger senderPeerID) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.senderPeerID = senderPeerID;
	}

	public GetPredecessorMessage(UUID mid, ChordNode thisNode) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = thisNode.getHost();
		this.senderPeerID = thisNode.getPeerID();
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

	public static ISerializer<GetPredecessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(GetPredecessorMessage findSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(findSuccessorMessage.sender, out);
			byte[] senderPeerIDByteArray = findSuccessorMessage.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);
		}

		@Override
		public GetPredecessorMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] senderPeerIDByteArray = new byte[size];
			in.readBytes(senderPeerIDByteArray);

			return new GetPredecessorMessage(mid, sender, new BigInteger(senderPeerIDByteArray));
		}
	};
}
