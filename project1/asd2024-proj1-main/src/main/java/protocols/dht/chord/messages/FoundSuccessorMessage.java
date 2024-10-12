package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FoundSuccessorMessage extends ProtoMessage {
	public static final short MSG_ID = 502;

	private final UUID mid;
	private final Host originalSender, sender;

	private final short toDeliver;
	private final BigInteger peerID;

	@Override
	public String toString() {
		return "LookupMessage{" +
				"mid=" + mid +
				'}';
	}

	public FoundSuccessorMessage(UUID mid, Host originalSender, Host sender, short toDeliver, BigInteger peerID) {
		super(MSG_ID);
		this.mid = mid;
		this.originalSender = originalSender;
		this.sender = sender;
		this.toDeliver = toDeliver;
		this.peerID = peerID;
	}

	public FoundSuccessorMessage(FindSuccessorMessage findSuccessorMessage, Host sender) {
		super(MSG_ID);
		this.mid = findSuccessorMessage.getMid();
		this.originalSender = findSuccessorMessage.getOriginalSender();
		this.sender = sender;
		this.toDeliver = findSuccessorMessage.getToDeliver();
		this.peerID = findSuccessorMessage.getPeerID();
	}

	public Host getOriginalSender() {
		return originalSender;
	}

	public Host getSender() {
		return sender;
	}

	public UUID getMid() {
		return mid;
	}

	public short getToDeliver() {
		return toDeliver;
	}

	public BigInteger getPeerID() {
		return peerID;
	}

	public static ISerializer<FoundSuccessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(FoundSuccessorMessage foundSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(foundSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(foundSuccessorMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(foundSuccessorMessage.originalSender, out);
			Host.serializer.serialize(foundSuccessorMessage.sender, out);
			out.writeShort(foundSuccessorMessage.toDeliver);
			byte[] peerIDByteArray = foundSuccessorMessage.peerID.toByteArray();
			out.writeInt(peerIDByteArray.length);
			out.writeBytes(peerIDByteArray);
		}

		@Override
		public FoundSuccessorMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host originalSender = Host.serializer.deserialize(in);
			Host sender = Host.serializer.deserialize(in);
			short toDeliver = in.readShort();
			int size = in.readInt();
			byte[] peerIDByteArray = new byte[size];
			in.readBytes(peerIDByteArray);

			return new FoundSuccessorMessage(mid, originalSender, sender, toDeliver, new BigInteger(peerIDByteArray));
		}
	};
}
