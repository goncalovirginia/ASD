package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class ReturnPredecessorMessage extends ProtoMessage {

    public static final short MSG_ID = 504;

    private final UUID mid;
    private final Host sender, predecessor;
    private final BigInteger senderPeerID, predecessorPeerID;

    @Override
    public String toString() {
        return "ReturnPredecessorMessage{" +
                "mid=" + mid +
                '}';
    }

    public ReturnPredecessorMessage(UUID mid, Host sender, Host predecessor, BigInteger senderPeerID, BigInteger predecessorPeerID) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.predecessor = predecessor;
        this.senderPeerID = senderPeerID;
        this.predecessorPeerID = predecessorPeerID;
    }

    public ReturnPredecessorMessage(UUID mid, short toDeliver, ChordNode thisNode, ChordNode predecessorNode) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = thisNode.getHost();
        this.predecessor = predecessorNode.getHost();
        this.senderPeerID = thisNode.getPeerID();
        this.predecessorPeerID = predecessorNode.getPeerID();
    }

    public Host getSender() {
        return sender;
    }

    public Host getPredecessor() {
        return predecessor;
    }

    public UUID getMid() {
        return mid;
    }

    public BigInteger getSenderPeerID() {
        return senderPeerID;
    }

    public BigInteger getPredecessorPeerID() {
        return predecessorPeerID;
    }

    public static ISerializer<ReturnPredecessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReturnPredecessorMessage message, ByteBuf out) throws IOException {
            out.writeLong(message.mid.getMostSignificantBits());
            out.writeLong(message.mid.getLeastSignificantBits());
            Host.serializer.serialize(message.sender, out);
            Host.serializer.serialize(message.predecessor, out);

            byte[] senderPeerIDByteArray = message.senderPeerID.toByteArray();
            out.writeInt(senderPeerIDByteArray.length);
            out.writeBytes(senderPeerIDByteArray);

            byte[] predecessorPeerIDByteArray = message.predecessorPeerID.toByteArray();
            out.writeInt(predecessorPeerIDByteArray.length);
            out.writeBytes(predecessorPeerIDByteArray);
        }

        @Override
        public ReturnPredecessorMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            Host predecessor = Host.serializer.deserialize(in);

            int senderPeerIDSize = in.readInt();
            byte[] senderPeerIDByteArray = new byte[senderPeerIDSize];
            in.readBytes(senderPeerIDByteArray);
            BigInteger senderPeerID = new BigInteger(1, senderPeerIDByteArray);

            int predecessorPeerIDSize = in.readInt();
            byte[] predecessorPeerIDByteArray = new byte[predecessorPeerIDSize];
            in.readBytes(predecessorPeerIDByteArray);
            BigInteger predecessorPeerID = new BigInteger(1, predecessorPeerIDByteArray);

            return new ReturnPredecessorMessage(mid, sender, predecessor, senderPeerID, predecessorPeerID);
        }
    };
}