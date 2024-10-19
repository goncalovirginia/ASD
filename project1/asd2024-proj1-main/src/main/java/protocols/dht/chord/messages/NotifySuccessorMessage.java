package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class NotifySuccessorMessage extends ProtoMessage {

	public static final short MSG_ID = 505;

	private final UUID mid;
	private final Host sender;

	private final BigInteger senderPeerID;

	@Override
	public String toString() {
		return "NotifySuccessorMessage{" +
				"mid=" + mid +
				'}';
	}

	public NotifySuccessorMessage(UUID mid, Host sender, BigInteger senderPeerID) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.senderPeerID = senderPeerID;
	}

	public NotifySuccessorMessage(UUID mid, ChordNode thisNode) {
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

	public static ISerializer<NotifySuccessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(NotifySuccessorMessage findSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(findSuccessorMessage.sender, out);
			byte[] senderPeerIDByteArray = findSuccessorMessage.senderPeerID.toByteArray();
			out.writeInt(senderPeerIDByteArray.length);
			out.writeBytes(senderPeerIDByteArray);
		}

		@Override
		public NotifySuccessorMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] senderPeerIDByteArray = new byte[size];
			in.readBytes(senderPeerIDByteArray);

			return new NotifySuccessorMessage(mid, sender, new BigInteger(1, senderPeerIDByteArray));
		}
	};
}
