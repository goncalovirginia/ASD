package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AddRemoveMessage extends ProtoMessage {

    public final static short MSG_ID = 506;

    public final static byte ADD = 0;
    public final static byte REMOVE = 1;

    private final Host replica;
    private final int opSeq;
    private final byte opType; // changed from boolean to byte

    public AddRemoveMessage(int opSeq, byte opType, Host replica) {
        super(MSG_ID);
        this.replica = replica;
        this.opSeq = opSeq;
        this.opType = opType;
    }

    public Host getReplica() {
        return replica;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public byte getOpType() {
        return opType;
    }

    @Override
    public String toString() {
        return "AddRemoveMessage{" +
                "OpSeq=" + opSeq +
                ", Replica=" + replica +
                ", opType=" + opType +
                '}';             
    }

    public static ISerializer<AddRemoveMessage> serializer = new ISerializer<AddRemoveMessage>() {
        @Override
        public void serialize(AddRemoveMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.opSeq);
            out.writeByte(msg.opType); // changed to write byte
            Host.serializer.serialize(msg.replica, out);
        }

        @Override
        public AddRemoveMessage deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            byte opType = in.readByte(); // changed to read byte
            Host replica = Host.serializer.deserialize(in);
            
            return new AddRemoveMessage(opSeq, opType, replica);
        }
    };
}
