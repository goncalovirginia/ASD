package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class Point2PointMessage extends ProtoMessage {
	public static final short MSG_ID = 401;

	private final UUID mid;
	private final Host sender, destination;
	private final byte[] senderPeerID, destinationID;
	private final byte[] content;

	@Override
	public String toString() {
		return "Point2PointMessage{" +
				"mid=" + mid +
				'}';
	}

	public Point2PointMessage(Send send, Host sender, Host destination) {
		super(MSG_ID);
		this.mid = send.getMessageID();
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = send.getSenderPeerID();
		this.destinationID = send.getDestinationPeerID();
		this.content = send.getMessagePayload();
	}

	public Point2PointMessage(UUID mid, Host sender, Host destination, byte[] senderPeerID, byte[] destinationID, byte[] content) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = senderPeerID;
		this.destinationID = destinationID;
		this.content = content;
	}

	public Point2PointMessage(HelperNodeMessage helperNodeMessage) {
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

	public byte[] getSenderPeerID() {
		return senderPeerID;
	}

	public byte[] getDestinationID() {
		return destinationID;
	}

	public byte[] getContent() {
		return content;
	}

	public static ISerializer<Point2PointMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(Point2PointMessage point2PointMessage, ByteBuf out) throws IOException {
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
		public Point2PointMessage deserialize(ByteBuf in) throws IOException {
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

			return new Point2PointMessage(mid, sender, destination, senderPeerID, destinationID, content);
		}
	};
}
