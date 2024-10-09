package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class Point2PointMessage extends ProtoMessage {
	public static final short MSG_ID = 401;

	private final UUID mid;
	private final Host sender;

	private final short toDeliver;
	private final byte[] content;

	@Override
	public String toString() {
		return "Point2PointMessage{" +
				"mid=" + mid +
				'}';
	}

	public Point2PointMessage(UUID mid, Host sender, short toDeliver, byte[] content) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.toDeliver = toDeliver;
		this.content = content;
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

	public byte[] getContent() {
		return content;
	}

	public static ISerializer<Point2PointMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(Point2PointMessage point2PointMessage, ByteBuf out) throws IOException {
			out.writeLong(point2PointMessage.mid.getMostSignificantBits());
			out.writeLong(point2PointMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(point2PointMessage.sender, out);
			out.writeShort(point2PointMessage.toDeliver);
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
			short toDeliver = in.readShort();
			int size = in.readInt();
			byte[] content = new byte[size];
			if (size > 0)
				in.readBytes(content);

			return new Point2PointMessage(mid, sender, toDeliver, content);
		}
	};
}
