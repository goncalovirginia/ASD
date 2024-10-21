package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class HelperNodeMessage extends ProtoMessage {
	public static final short MSG_ID = 402;


	private final UUID mid;
	private final Host sender, destination, helper, preHelper;
	private final byte[] senderPeerID, destinationID, helperID, preHelperID;
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

	public HelperNodeMessage(UUID mid, Host sender, Host destination, Host helper, Host preHelper, byte[] senderPeerID, byte[] destinationID, byte[] helperID, byte[] preID, byte[] content) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.destination = destination;
		this.helper = helper;
		this.preHelper = preHelper;

		this.senderPeerID = senderPeerID;
		this.destinationID = destinationID;
		this.helperID = helperID;
		this.preHelperID = preID;
		this.content = content;
	}

	public HelperNodeMessage(Point2PointMessage point2PointMessage, byte[] helperID, Host helperHost, byte[] preHelperID, Host preHelper) {
		super(MSG_ID);
		this.mid = point2PointMessage.getMid();
		this.sender = point2PointMessage.getSender();
		this.destination = point2PointMessage.getDestination();
		this.helper = helperHost;
		this.preHelper = preHelper;

		this.senderPeerID = point2PointMessage.getSenderPeerID();
		this.destinationID = point2PointMessage.getDestinationID();
		this.helperID = helperID;
		this.preHelperID = preHelperID;

		this.content = point2PointMessage.getContent();
	}

	public Host getSender() {
		return sender;
	}

	public Host getDestination() {
		return destination;
	}

	public Host getHelper() {
		return helper;
	}

	public Host getPreHelper() {
		return preHelper;
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
		public void serialize(HelperNodeMessage helperMessage, ByteBuf out) throws IOException {
			out.writeLong(helperMessage.mid.getMostSignificantBits());
			out.writeLong(helperMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(helperMessage.sender, out);
			Host.serializer.serialize(helperMessage.destination, out);
			Host.serializer.serialize(helperMessage.helper, out);
			Host.serializer.serialize(helperMessage.preHelper, out);
			
			out.writeInt(helperMessage.senderPeerID.length);
			out.writeBytes(helperMessage.senderPeerID);
			
			out.writeInt(helperMessage.destinationID.length);
			out.writeBytes(helperMessage.destinationID);

			out.writeInt(helperMessage.helperID.length);
			out.writeBytes(helperMessage.helperID);

			out.writeInt(helperMessage.preHelperID.length);
			out.writeBytes(helperMessage.preHelperID);

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
			Host helperHost = Host.serializer.deserialize(in);
			Host preHelperHost = Host.serializer.deserialize(in);

			int senderPeerIDSize = in.readInt();
			byte[] senderPeerID = new byte[senderPeerIDSize];
			if (senderPeerIDSize > 0)
				in.readBytes(senderPeerID);

			int destinationIDSize = in.readInt();
			byte[] destinationID = new byte[destinationIDSize];
			if (destinationIDSize > 0)
				in.readBytes(destinationID);

			int helperIDSize = in.readInt();
			byte[] helperID = new byte[helperIDSize];
			if (helperIDSize > 0)
				in.readBytes(helperID);

			int preHelperIDSize = in.readInt();
			byte[] preID = new byte[preHelperIDSize];
			if (preHelperIDSize > 0)
				in.readBytes(preID);

			int contentSize = in.readInt();
			byte[] content = new byte[contentSize];
			if (contentSize > 0)
				in.readBytes(content);

			return new HelperNodeMessage(mid, sender, destination, helperHost, preHelperHost, senderPeerID, destinationID, helperID, preID, content);
		}
	};
}
