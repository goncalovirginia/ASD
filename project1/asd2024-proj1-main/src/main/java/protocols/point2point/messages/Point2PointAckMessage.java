package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class Point2PointAckMessage extends ProtoMessage {
	public static final short MSG_ID = 403;

	private final UUID mid;
	private final Host sender, destination;
	private final BigInteger senderPeerID, destinationID;

	@Override
	public String toString() {
		return "Point2PointAckMessage{" +
				"mid=" + mid + " sender=" + sender + " destination=" + destination +
				'}';
	}

	public Point2PointAckMessage(UUID mid, Host sender, Host destination, BigInteger senderPeerID, BigInteger destinationID) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.destination = destination;
		this.senderPeerID = senderPeerID;
		this.destinationID = destinationID;
	}

	public Point2PointAckMessage(Point2PointMessage point2PointMessage) {
		super(MSG_ID);
		this.mid = point2PointMessage.getMid();
		this.sender = point2PointMessage.getDestination();
		this.destination = point2PointMessage.getSender();
		this.senderPeerID = point2PointMessage.getDestinationID();
		this.destinationID = point2PointMessage.getSenderPeerID();
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

	public static ISerializer<Point2PointAckMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(Point2PointAckMessage helperMessage, ByteBuf out) throws IOException {
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
		}

		@Override
		public Point2PointAckMessage deserialize(ByteBuf in) throws IOException {
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

			return new Point2PointAckMessage(mid, sender, destination, new BigInteger(1, senderPeerID), new BigInteger(1, destinationID));
		}
	};
}
