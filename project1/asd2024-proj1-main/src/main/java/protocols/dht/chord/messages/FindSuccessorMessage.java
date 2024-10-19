package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindSuccessorMessage extends ProtoMessage {

	public static final short MSG_ID = 501;

	private final UUID mid;
	private final Host originalSender, sender;

	private final BigInteger key;

	@Override
	public String toString() {
		return "FindSuccessorMessage{" +
				"mid=" + mid +
				'}';
	}

	public FindSuccessorMessage(UUID mid, Host originalSender, Host sender, BigInteger key) {
		super(MSG_ID);
		this.mid = mid;
		this.originalSender = originalSender;
		this.sender = sender;
		this.key = key;
	}

	public FindSuccessorMessage(FindSuccessorMessage findSuccessorMessage, Host sender) {
		super(MSG_ID);
		this.mid = findSuccessorMessage.getMid();
		this.originalSender = findSuccessorMessage.getOriginalSender();
		this.sender = sender;
		this.key = findSuccessorMessage.getKey();
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

	public BigInteger getKey() {
		return key;
	}

	public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(FindSuccessorMessage findSuccessorMessage, ByteBuf out) throws IOException {
			out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
			out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(findSuccessorMessage.originalSender, out);
			Host.serializer.serialize(findSuccessorMessage.sender, out);
			byte[] peerIDByteArray = findSuccessorMessage.key.toByteArray();
			out.writeInt(peerIDByteArray.length);
			out.writeBytes(peerIDByteArray);
		}

		@Override
		public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host originalSender = Host.serializer.deserialize(in);
			Host sender = Host.serializer.deserialize(in);
			int size = in.readInt();
			byte[] peerIDByteArray = new byte[size];
			in.readBytes(peerIDByteArray);

			return new FindSuccessorMessage(mid, originalSender, sender, new BigInteger(1, peerIDByteArray));
		}
	};
}
