package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class HelperNodeMessage extends ProtoMessage {
	public static final short MSG_ID = 402;

	private final UUID mid;
	private final Host sender, destination;
	private final byte[] senderPeerID, destinationID;
	private final byte[] content;

	@Override
	public String toString() {
		return "HelperNodeMessage{" +
				"mid=" + mid +
				'}';
	}

	public HelperNodeMessage(Send send, Host sender, Host destination) {
		super(MSG_ID);
		this.mid = send.getMessageID();
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = send.getSenderPeerID();
		this.destinationID = send.getDestinationPeerID();
		this.content = send.getMessagePayload();
	}

	public HelperNodeMessage(UUID mid, Host sender, Host destination, byte[] senderPeerID, byte[] destinationID, byte[] content) {
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

	public Host getSender() {
		return sender;
	}

	public Host getDestination() {
		return destination;
	}

	public UUID getMid() {
		return mid;
	}

	public byte[] getSenderPeerID() {
		return senderPeerID;
	}

	public byte[] getDestinationID() {
		return destinationID;
	}

	public byte[] getContent() {
		return content;
	}

	public static ISerializer<HelperNodeMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(HelperNodeMessage point2PointMessage, ByteBuf out) throws IOException {
			out.writeLong(point2PointMessage.mid.getMostSignificantBits());
			out.writeLong(point2PointMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(point2PointMessage.sender, out);
			Host.serializer.serialize(point2PointMessage.destination, out);
			out.writeInt(point2PointMessage.senderPeerID.length);
			out.writeBytes(point2PointMessage.senderPeerID);
			out.writeInt(point2PointMessage.destinationID.length);
			out.writeBytes(point2PointMessage.destinationID);
			out.writeInt(point2PointMessage.content.length);
			if (point2PointMessage.content.length > 0) {
				out.writeBytes(point2PointMessage.content);
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

			return new HelperNodeMessage(mid, sender, destination, senderPeerID, destinationID, content);
		}
	};
}
