package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class Point2PointMessage extends ProtoMessage {

	public static final short MSG_ID = 401;

	private final UUID mid;
	private final Host sender, destination;
	private final BigInteger senderPeerID, destinationID;
	private final byte[] content;

	@Override
	public String toString() {
		return "Point2PointMessage{" +
				"mid=" + mid + " sender=" + sender + " destination=" + destination + " content=" + new String(content) +
				'}';
	}

	public Point2PointMessage(Send send, Host sender, Host destination) {
		super(MSG_ID);
		this.mid = send.getMessageID();
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = new BigInteger(1, send.getSenderPeerID());
		this.destinationID = new BigInteger(1, send.getDestinationPeerID());
		this.content = send.getMessagePayload();
	}

	public Point2PointMessage(UUID mid, Host sender, Host destination, BigInteger senderPeerID, BigInteger destinationID, byte[] content) {
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

	public Point2PointMessage(Point2PointAckMessage point2PointAckMessage) {
		super(MSG_ID);
		this.mid = point2PointAckMessage.getMid();
		this.sender = point2PointAckMessage.getSender();
		this.destination = point2PointAckMessage.getDestination();
		this.senderPeerID = point2PointAckMessage.getSenderPeerID();
		this.destinationID = point2PointAckMessage.getDestinationID();
		this.content = null;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		} else if (!(o instanceof Point2PointMessage)) {
			return false;
		} else {
			Point2PointMessage point2PointMessage = (Point2PointMessage) o;
			return point2PointMessage.mid.equals(this.mid);
		}
	}

	@Override
	public int hashCode() {
		return mid.hashCode();
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

	public static ISerializer<Point2PointMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(Point2PointMessage point2PointMessage, ByteBuf out) throws IOException {
			out.writeLong(point2PointMessage.mid.getMostSignificantBits());
			out.writeLong(point2PointMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(point2PointMessage.sender, out);
			Host.serializer.serialize(point2PointMessage.destination, out);

			byte[] sendByteArray = point2PointMessage.senderPeerID.toByteArray();
			out.writeInt(sendByteArray.length);
			out.writeBytes(sendByteArray);

			byte[] destByteArray = point2PointMessage.destinationID.toByteArray();
			out.writeInt(destByteArray.length);
			out.writeBytes(destByteArray);

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

			return new Point2PointMessage(mid, sender, destination, new BigInteger(1, senderPeerID), new BigInteger(1, destinationID), content);
		}
	};
}
