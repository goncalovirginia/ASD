package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class P2PReceivedMessage extends ProtoMessage {
	public static final short MSG_ID = 412;


	private final UUID mid;
	private final Host sender, destination;
	private final BigInteger senderPeerID, destinationID;
	private final byte[] content;

	@Override
	public String toString() {
		return "P2PReceivedMessage{" +
				"mid=" + mid +
				'}';
	}

	//I'll comment DELETE later - if it is to be delete (late change - predecessor and sender)
/* 	public P2PReceivedMessage(Send send, Host sender, Host destination, Host helper) {
		super(MSG_ID);
		this.mid = send.getMessageID();
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = send.getSenderPeerID();
		this.destinationID = send.getDestinationPeerID();
		this.content = send.getMessagePayload();
	} */

	public P2PReceivedMessage(UUID mid, Host sender, Host destination, BigInteger senderPeerID, BigInteger destinationID, byte[] content) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.destination = destination;

		this.senderPeerID = senderPeerID;
		this.destinationID = destinationID;

		this.content = content;
	}

	public P2PReceivedMessage(HelperNodeMessage helperNodeMessage) {
		super(MSG_ID);
		this.mid = helperNodeMessage.getMid();
		this.sender = helperNodeMessage.getSender();
		this.destination = helperNodeMessage.getDestination();

		this.senderPeerID = helperNodeMessage.getSenderPeerID();
		this.destinationID = helperNodeMessage.getDestinationID();

		this.content = helperNodeMessage.getContent();
	}

	public Host getSender() {
		return sender;
	}

	public Host getDestination() {
		return destination;
	}

	public UUID getMid() {
		return mid;
	}

	public BigInteger getSenderPeerID() {
		return senderPeerID;
	}

	public BigInteger getDestinationID() {
		return destinationID;
	}

	public byte[] getContent() {
		return content;
	}

	public static ISerializer<P2PReceivedMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(P2PReceivedMessage helperMessage, ByteBuf out) throws IOException {
			out.writeLong(helperMessage.mid.getMostSignificantBits());
			out.writeLong(helperMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(helperMessage.sender, out);
			Host.serializer.serialize(helperMessage.destination, out);

			byte[] senderByteArray = helperMessage.senderPeerID.toByteArray();
			out.writeInt(senderByteArray.length);
			out.writeBytes(senderByteArray);

			byte[] destByteArray = helperMessage.destinationID.toByteArray();
			out.writeInt(destByteArray.length);
			out.writeBytes(destByteArray);


			out.writeInt(helperMessage.content.length);
			if (helperMessage.content.length > 0) {
				out.writeBytes(helperMessage.content);
			}
		}

		@Override
		public P2PReceivedMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			Host destination = Host.serializer.deserialize(in);

			int senderPeerIDSize = in.readInt();
			byte[] senderPeerID = new byte[senderPeerIDSize];
			if (senderPeerIDSize > 0)
				in.readBytes(senderPeerID);

			int destinationIDSize = in.readInt();
			byte[] destinationID = new byte[destinationIDSize];
			if (destinationIDSize > 0)
				in.readBytes(destinationID);

			int contentSize = in.readInt();
			byte[] content = new byte[contentSize];
			if (contentSize > 0)
				in.readBytes(content);

			return new P2PReceivedMessage(mid, sender, destination, new BigInteger(1, senderPeerID), new BigInteger(1, destinationID), content);
		}
	};
}
