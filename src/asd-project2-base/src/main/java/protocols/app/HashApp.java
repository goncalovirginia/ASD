package protocols.app;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleServerChannel;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientDownEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientUpEvent;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* import protocols.abd.ABD;
import protocols.abd.renotifications.ReadCompleteNotification;
import protocols.abd.renotifications.UpdateValueNotification;
import protocols.abd.renotifications.WriteCompleteNotification;
import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest; */
import protocols.app.messages.RequestMessage;
import protocols.app.messages.ResponseMessage;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.app.utils.Operation;
import protocols.statemachine.StateMachine;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HashApp extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(HashApp.class);

	//Protocol information, to register in babel
	public static final String PROTO_NAME = "HashApp";
	public static final short PROTO_ID = 300;

	//Application state
	private int executedOps;
	private final Map<String, byte[]> data;
	private byte[] cumulativeHash;

	//Client callbacks
	private final Map<UUID, Pair<Host, Long>> clientIdMapper;

	public static  enum ReplicationStrategy {SMR, ABD};

	public final ReplicationStrategy strategy;

	public HashApp(Properties properties) throws HandlerRegistrationException, IOException {
		super(PROTO_NAME, PROTO_ID);

		executedOps = 0;
		data = new HashMap<>();
		clientIdMapper = new TreeMap<>();
		cumulativeHash = new byte[0];

		String address = properties.getProperty("address");
		String port = properties.getProperty("server_port");
		String s = properties.getProperty("replication_strategy", "SMR");

		switch(s) {
		case "ABD":
			this.strategy = ReplicationStrategy.ABD;
			break;
		case "SMR":
		default:
			this.strategy = ReplicationStrategy.SMR;        
		}

		logger.info("Listening on {}:{}", address, port);

		//We are using a ServerChannel here, which does not create connections,
		// only listens for incoming client connections.
		Properties channelProps = new Properties();
		channelProps.setProperty(SimpleServerChannel.ADDRESS_KEY, address);
		channelProps.setProperty(SimpleServerChannel.PORT_KEY, port);
		channelProps.setProperty(SimpleServerChannel.HEARTBEAT_INTERVAL_KEY, "1000");
		channelProps.setProperty(SimpleServerChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
		channelProps.setProperty(SimpleServerChannel.CONNECT_TIMEOUT_KEY, "1000");
		int channelId = createChannel(SimpleServerChannel.NAME, channelProps);

		//This channel has only two events - ClientUp and ClientDown (and, obviously, receiving messages)
		registerChannelEventHandler(channelId, ClientUpEvent.EVENT_ID, this::onClientUp);
		registerChannelEventHandler(channelId, ClientDownEvent.EVENT_ID, this::onClientDown);

		/*-------------------- Register Message Serializers ----------------------- */
		registerMessageSerializer(channelId, RequestMessage.MSG_ID, RequestMessage.serializer);
		registerMessageSerializer(channelId, ResponseMessage.MSG_ID, ResponseMessage.serializer);

		/*-------------------- Register Message Handlers -------------------------- */
		registerMessageHandler(channelId, RequestMessage.MSG_ID, this::uponRequestMessage);
		//We never receive a ResponseMessage, so just register the failure handler.
		registerMessageHandler(channelId, ResponseMessage.MSG_ID, null, this::uponMsgFail);

		/*-------------------- Register Execute Notification Handler --------------- */
		subscribeNotification(ExecuteNotification.NOTIFICATION_ID, this::uponExecuteNotification); //For Paxos interaction
/* 		subscribeNotification(ReadCompleteNotification.NOTIFICATION_ID, this::uponReadCompleteNotification); //For ABD interaction
		subscribeNotification(WriteCompleteNotification.NOTIFICATION_ID, this::uponWriteCompleteNotification); //For ABD interaction
		subscribeNotification(UpdateValueNotification.NOTIFICATION_ID, this::uponUpdateValueNotification); //For ABD interaction */

		/*-------------------- Register Request Handler ---------------------------- */
		registerRequestHandler(CurrentStateRequest.REQUEST_ID, this::uponCurrentStateRequest);
		registerRequestHandler(InstallStateRequest.REQUEST_ID, this::uponInstallStateRequest);

	}

	//Auxiliar method that exposes the current stack being employed.
	public final ReplicationStrategy getReplicationStrategy() {
		return this.strategy;
	}

	@Override
	public void init(Properties props) {
	}

	private void uponCurrentStateRequest(CurrentStateRequest req, short sourceProto) {
		byte[] state;
		try {
			state = this.getCurrentState();
		} catch (IOException e) {
			throw new AssertionError("Could not get current state of the application.", e);
		}
		sendReply(new CurrentStateReply(req.getInstance(), state), sourceProto);
	}

	private void uponInstallStateRequest(InstallStateRequest req, short sourceProto) {
		try {
			this.installState(req.getState());
			logger.info("State installed N_OPS= {}, MAP_SIZE={}, HASH={}",
					executedOps, data.size(), Hex.encodeHexString(cumulativeHash));
		} catch (IOException e) {
			throw new AssertionError("Failed in installing a new state on the application.", e);
		}
	}

	private void uponRequestMessage(RequestMessage msg, Host host, short sourceProto, int channelId) {
		logger.debug("Request received: " + msg + " from " + host);
		logger.info("requesting...");
		UUID opUUID = UUID.randomUUID();
		clientIdMapper.put(opUUID, Pair.of(host, msg.getOpId()));
		if(strategy == ReplicationStrategy.SMR) {
			//State machine replication + Paxos
			Operation op = new Operation(msg.getOpType(), msg.getKey(), msg.getData());
			try {
				sendRequest(new OrderRequest(opUUID, op.toByteArray()), StateMachine.PROTOCOL_ID);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
			//ABD interaction
/* 			if(msg.getOpType() == RequestMessage.READ) {

				sendRequest(new ReadRequest(opUUID, msg.getKey().getBytes()), ABD.PROTOCOL_ID);

			} else if (msg.getOpType() == RequestMessage.WRITE) {

				sendRequest(new WriteRequest(opUUID, msg.getKey().getBytes(), msg.getData()), ABD.PROTOCOL_ID);

			} else {
				System.err.println("Invalid client operation");
				System.exit(1);
			} */
		}
	}

	//This handler is executed for the paxos stack 
	private void uponExecuteNotification(ExecuteNotification not, short sourceProto) {
		try {
			//Deserialize operation received
			Operation op = Operation.fromByteArray(not.getOperation());

			cumulativeHash = appendOpToHash(cumulativeHash, op.getData());

			logger.debug("Executing: " + op);
			//Execute if it is a write operation
			if (op.getOpType() == RequestMessage.WRITE)
				data.put(op.getKey(), op.getData());
			executedOps++;
			if (executedOps % 10000 == 0) {
				logger.info("Current state N_OPS= {}, MAP_SIZE={}, HASH={}",
						executedOps, data.size(), Hex.encodeHexString(cumulativeHash));
			}
			//Check if the operation was issued by me
			Pair<Host, Long> pair = clientIdMapper.remove(not.getOpId());
			
			if (pair != null) {
				logger.info("Generating response to the client: instance - {}, opId - {}", executedOps, not.getOpId());
				//Generate a response to the client
				ResponseMessage resp;
				if (op.getOpType() == RequestMessage.WRITE)
					resp = new ResponseMessage(pair.getRight(), new byte[0]);
				else
					resp = new ResponseMessage(pair.getRight(), data.getOrDefault(op.getKey(), new byte[0]));
				//Respond
				sendMessage(resp, pair.getLeft());
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

/* 	//The following 3 handlers are are executed only for the abd stack
	private void uponReadCompleteNotification(ReadCompleteNotification not, short sourceProto) {
		String key = new String(not.getKey(),0,not.getKey().length);
		this.data.put(key, not.getValue());
		
		Pair<Host, Long> pair = clientIdMapper.remove(not.getOpId());
		
		sendMessage(new ResponseMessage(pair.getRight(), data.getOrDefault(key, new byte[0])), pair.getLeft());

		this.updateOperationCountAndPrintHash();
	}

	private void uponWriteCompleteNotification(WriteCompleteNotification not, short sourceProto) {
		String key = new String(not.getKey(),0,not.getKey().length);
		this.data.put(key, not.getValue());
		
		Pair<Host, Long> pair = clientIdMapper.remove(not.getOpId());
		
		sendMessage(new ResponseMessage(pair.getRight(), new byte[0]), pair.getLeft());
		
		this.updateOperationCountAndPrintHash();
	}

	private void uponUpdateValueNotification(UpdateValueNotification not, short sourceProto) {
		logger.debug("Updating key due to a remote update.");
		data.put(new String(not.getKey(),0,not.getKey().length), not.getValue());

		this.updateOperationCountAndPrintHash();
	} 


	private void updateOperationCountAndPrintHash() {
		executedOps++;

		if (executedOps % 10000 == 0) {
			cumulativeHash = computeDataHash().getBytes();
			logger.info("Current state N_OPS= {}, MAP_SIZE={}, HASH={}",
					executedOps, data.size(), Hex.encodeHexString(cumulativeHash));
		}
	}

	*/

	private byte[] appendOpToHash(byte[] hash, byte[] op) {
		MessageDigest mDigest;
		try {
			mDigest = MessageDigest.getInstance("sha-256");
		} catch (NoSuchAlgorithmException e) {
			logger.error("sha-256 not available...");
			throw new AssertionError("sha-256 not available...");
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write(hash);
			baos.write(op);
			return mDigest.digest(baos.toByteArray());
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new AssertionError();
		}
	}


	private String computeDataHash() {
		MessageDigest mDigest;
		try {
			mDigest = MessageDigest.getInstance("sha-256");
		} catch (NoSuchAlgorithmException e) {
			logger.error("sha-256 not available...");
			throw new AssertionError("sha-256 not available...");
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			TreeSet<String> orderedKeys = new TreeSet<String>(data.keySet());
			for (String key: orderedKeys) {
				dos.writeUTF(key);
				dos.write(data.get(key));
			}
			byte[] hash = mDigest.digest(baos.toByteArray());
			return Hex.encodeHexString(hash);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new AssertionError();
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		//If a message fails to be sent, for whatever reason, log the message and the reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	private void onClientUp(ClientUpEvent event, int channel) {
		logger.info(event);
	}

	private void onClientDown(ClientDownEvent event, int channel) {
		logger.info(event);
	}

	private byte[] getCurrentState() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeInt(executedOps);
		dos.writeInt(cumulativeHash.length);
		dos.write(cumulativeHash);
		dos.writeInt(data.size());
		for (Map.Entry<String, byte[]> entry : data.entrySet()) {
			dos.writeUTF(entry.getKey());
			dos.writeInt(entry.getValue().length);
			dos.write(entry.getValue());
		}
		return baos.toByteArray();
	}

	private void installState(byte[] newState) throws IOException {
		data.clear();
		ByteArrayInputStream bais = new ByteArrayInputStream(newState);
		DataInputStream dis = new DataInputStream(bais);
		executedOps = dis.readInt();
		cumulativeHash = new byte[dis.readInt()];
		dis.read(cumulativeHash);
		int mapSize = dis.readInt();
		for (int i = 0; i < mapSize; i++) {
			String key = dis.readUTF();
			byte[] value = new byte[dis.readInt()];
			dis.read(value);
			data.put(key, value);
		}
	}

}
