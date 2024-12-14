package protocols.abd.messages;

import java.io.IOException;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class AckAddRemoveMessage extends ProtoMessage {

    public final static short MSG_ID = 507;

    public final static byte ADD = 0;
    public final static byte REMOVE = 1;

    private final int opSeq;
    private final Host replica;
    private final byte opType; // changed from boolean to byte

    public AckAddRemoveMessage(int opSeq, byte opType, Host replica) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.replica = replica;
        this.opType = opType;
    }

    public int getOpId() {
        return opSeq;
    }

    public Host getReplica() {
        return replica;
    }

    public byte getOpType() {
        return opType;
    }

    @Override
    public String toString() {
        return "AckAddRemoveMessage{" +
                "opSeq=" + opSeq +
                ", replica=" + replica +
                ", opType=" + opType +
                '}';
    }

    public static ISerializer<AckAddRemoveMessage> serializer = new ISerializer<AckAddRemoveMessage>() {
        @Override
        public void serialize(AckAddRemoveMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.opSeq);
            Host.serializer.serialize(msg.replica, out);
            out.writeByte(msg.opType); // changed to write byte
        }

        @Override
        public AckAddRemoveMessage deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            Host replica = Host.serializer.deserialize(in);
            byte opType = in.readByte(); // changed to read byte
            return new AckAddRemoveMessage(opSeq, opType, replica);
        }
    };
}
