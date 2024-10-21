package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class HelperNodeMessage extends ProtoMessage {
	public static final short MSG_ID = 402;


	private final UUID mid;
	private final Host sender, destination;
	private final BigInteger senderPeerID, destinationID;
	private final byte[] content;

	@Override
	public String toString() {
		return "HelperNodeMessage{" +
				"mid=" + mid +
				'}';
	}

	//I'll comment DELETE later - if it is to be delete (late change - predecessor and sender)
/* 	public HelperNodeMessage(Send send, Host sender, Host destination, Host helper) {
		super(MSG_ID);
		this.mid = send.getMessageID();
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = send.getSenderPeerID();
		this.destinationID = send.getDestinationPeerID();
		this.content = send.getMessagePayload();
	} */

	public HelperNodeMessage(UUID mid, Host sender, Host destination, BigInteger senderPeerID, BigInteger destinationID, byte[] content) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.destination = destination;

		this.senderPeerID = senderPeerID;
		this.destinationID = destinationID;

		this.content = content;
	}

	public HelperNodeMessage(Point2PointMessage point2PointMessage) {
		super(MSG_ID);
		this.mid = point2PointMessage.getMid();
		this.sender = point2PointMessage.getSender();
		this.destination = point2PointMessage.getDestination();

		this.senderPeerID = point2PointMessage.getSenderPeerID();
		this.destinationID = point2PointMessage.getDestinationID();

		this.content = point2PointMessage.getContent();
	}

	public HelperNodeMessage(P2PReceivedMessage point2PointMessage) {
		super(MSG_ID);
		this.mid = point2PointMessage.getMid();
		this.sender = point2PointMessage.getSender();
		this.destination = point2PointMessage.getDestination();

		this.senderPeerID = point2PointMessage.getSenderPeerID();
		this.destinationID = point2PointMessage.getDestinationID();

		this.content = point2PointMessage.getContent();
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

	public static ISerializer<HelperNodeMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(HelperNodeMessage helperMessage, ByteBuf out) throws IOException {
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
		public HelperNodeMessage deserialize(ByteBuf in) throws IOException {
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

			return new HelperNodeMessage(mid, sender, destination, new BigInteger(1, senderPeerID), new BigInteger(1, destinationID), content);
		}
	};
}
