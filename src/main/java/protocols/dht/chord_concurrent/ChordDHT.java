package protocols.dht.chord_concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.AutomatedApp;
import protocols.dht.chord_concurrent.messages.*;
import protocols.dht.chord_concurrent.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord_concurrent.replies.LookupReply;
import protocols.dht.chord_concurrent.requests.LookupRequest;
import protocols.dht.chord_concurrent.timers.FixFingersTimer;
import protocols.dht.chord_concurrent.timers.RetryTCPConnectionsTimer;
import protocols.dht.chord_concurrent.timers.StabilizeTimer;
import protocols.point2point.notifications.DHTInitializedNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.AuxCalcs;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ChordDHT extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(ChordDHT.class);

	public static final short PROTOCOL_ID = 500;
	public static final String PROTOCOL_NAME = "ChordDHT";

//	private final short COMM_PROTOCOL_ID;

	private final int tcpChannelId;
	private final Set<Host> pendingHostConnections;

	private ChordNode predecessorNode;
	private final ChordNode thisNode;
	private final Finger[] fingers;

	private final Set<UUID> pendingLookupRequests;
	private final Map<UUID, Finger> fingersPendingSuccessor;

	private boolean isInitialized;

	public ChordDHT(Properties properties, Host thisHost, short commProtocolID) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);

		//COMM_PROTOCOL_ID = commProtocolID;

		pendingHostConnections = new HashSet<>();
		pendingLookupRequests = new HashSet<>();
		fingersPendingSuccessor = new HashMap<>();
		isInitialized = false;

		//initialize thisNode and predecessorNode
		String myPeerIDHex = properties.getProperty(AutomatedApp.PROPERTY_NODE_ID);
		BigInteger myPeerID = new BigInteger(myPeerIDHex, 16);
		thisNode = new ChordNode(myPeerID, thisHost);
		predecessorNode = thisNode;

		//initialize fingers
		int numFingers = AuxCalcs.log2Ceil(Integer.parseInt(properties.getProperty("id_bits")));
		fingers = new Finger[numFingers];
		BigInteger fingerEnd = thisNode.getPeerID().add(BigInteger.TWO.pow(fingers.length)).mod(BigInteger.TWO.pow(fingers.length));
		for (int i = fingers.length-1; i >= 0; i--) {
			//(thisNode.getPeerID() + 2^i) mod 2^fingers.length
			BigInteger fingerStart = thisNode.getPeerID().add(BigInteger.TWO.pow(i)).mod(BigInteger.TWO.pow(fingers.length));
			fingers[i] = new Finger(fingerStart, fingerEnd, thisNode);
			fingerEnd = fingerStart;
		}

		//register TCP channel
		Properties tcpChannelProperties = new Properties();
		tcpChannelProperties.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); //The address to bind to
		tcpChannelProperties.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")); //The port to bind to
		tcpChannelProperties.setProperty(TCPChannel.METRICS_INTERVAL_KEY, properties.getProperty("channel_metrics_interval", "10000")); //The interval to receive channel metrics
		tcpChannelProperties.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
		tcpChannelProperties.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
		tcpChannelProperties.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
		tcpChannelId = createChannel(TCPChannel.NAME, tcpChannelProperties); //Create the channel with the given properties

		//register TCP channel events
		registerChannelEventHandler(tcpChannelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(tcpChannelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(tcpChannelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(tcpChannelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(tcpChannelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

		//register request handlers
		registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

		//register message serializers
		registerMessageSerializer(tcpChannelId, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);
		registerMessageSerializer(tcpChannelId, FoundSuccessorMessage.MSG_ID, FoundSuccessorMessage.serializer);
		registerMessageSerializer(tcpChannelId, GetPredecessorMessage.MSG_ID, GetPredecessorMessage.serializer);
		registerMessageSerializer(tcpChannelId, ReturnPredecessorMessage.MSG_ID, ReturnPredecessorMessage.serializer);
		registerMessageSerializer(tcpChannelId, NotifySuccessorMessage.MSG_ID, NotifySuccessorMessage.serializer);

		//register message handlers
		registerMessageHandler(tcpChannelId, FindSuccessorMessage.MSG_ID, this::uponFindSuccessorMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, FoundSuccessorMessage.MSG_ID, this::uponFoundSuccessorMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, GetPredecessorMessage.MSG_ID, this::uponGetPredecessorMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, ReturnPredecessorMessage.MSG_ID, this::uponReturnPredecessorMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, NotifySuccessorMessage.MSG_ID, this::uponNotifyPredecessorMessage, this::uponMessageFail);

		//register timer handlers
		registerTimerHandler(RetryTCPConnectionsTimer.TIMER_ID, this::retryTCPConnections);
		registerTimerHandler(StabilizeTimer.TIMER_ID, this::stabilize);
		registerTimerHandler(FixFingersTimer.TIMER_ID, this::fixFingers);
	}

	@Override
	public void init(Properties props) {
		//inform the point2point algorithm above about the TCP channel to use
		//triggerNotification(new TCPChannelCreatedNotification(tcpChannelId));

		//initiate timers
		//setupPeriodicTimer(new RetryTCPConnectionsTimer(), 1000, 1000);
		
		//setupPeriodicTimer(new StabilizeTimer(), 1000, 1000);
		//setupPeriodicTimer(new FixFingersTimer(), 1000, 1000);

		//setupPeriodicTimer(new StabilizeTimer(), 1000000000, 1000000000);
		//setupPeriodicTimer(new FixFingersTimer(), 1000000000, 1000000000);

		//establish TCP connection to contact host
		if (props.containsKey("contact")) {
			connectToHost(props.getProperty("contact"));
		}
	}

	private void connectToHost(String contact) {
		try {
			String[] hostElems = contact.split(":");
			Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
			//We add to the pending set until the connection is successful
			pendingHostConnections.add(contactHost);
			openConnection(contactHost);
		} catch (Exception e) {
			logger.error("Invalid contact on configuration: " + contact);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void joinStep1(Host contactHost) {
		/* FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), thisNode.getHost(), thisNode.getHost(), thisNode.getPeerID());
		sendMessage(findSuccessorMessage, contactHost); */
	}

	private void joinStep2(FoundSuccessorMessage foundSuccessorMessage) {
		ChordNode senderNode = new ChordNode(foundSuccessorMessage.getSenderPeerID(), foundSuccessorMessage.getSenderHost());
		ChordNode successorNode = new ChordNode(foundSuccessorMessage.getSuccessorPeerID(), foundSuccessorMessage.getSuccessorHost());

		predecessorNode = senderNode;
		fingers[0].setChordNode(successorNode);

		isInitialized = true;
		triggerNotification(new DHTInitializedNotification());
	}

	private void updateFingerNode(FoundSuccessorMessage foundSuccessorMessage) {
		ChordNode newSuccessorNode = new ChordNode(foundSuccessorMessage.getSenderPeerID(), foundSuccessorMessage.getSenderHost());
		fingersPendingSuccessor.remove(foundSuccessorMessage.getMid()).setChordNode(newSuccessorNode);;
	}

	//TODO: kind of optional, but good to have:
	//TODO: after the node is inserted in the network and the finger tables are stabalized, do the last step and move any outdated (key, value) pairs..
	//TODO: ..stored in the node's immediate successor, to this node (deleting them in the successor)
	private void moveKeysFromSuccessor() {

	}

	private ChordNode closestPrecedingNode(BigInteger peerID) {
		for (int i = fingers.length-1; i >= 0 ; i--) {
			if (fingers[i].isInInterval(peerID)) {
				return fingers[i].getChordNode();
			}
		}
		return thisNode;
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponLookupRequest(LookupRequest request, short protoID) {
		logger.info("Received LookupRequest: " + request.toString());

		FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(request.getMid(), thisNode.getHost(), thisNode.getHost(), request.getPeerIDNumerical());
		uponFindSuccessorMessage(findSuccessorMessage, thisNode.getHost(), protoID, tcpChannelId);
		pendingLookupRequests.add(request.getMid());
	}

	/*--------------------------------- Messages ---------------------------------------- */

	//TBD message to UPDATE the new Node and sucessor
	//have a message with type 0 or 1
	//

	private void uponFindSuccessorMessage(FindSuccessorMessage findSuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received LookupMessage: " + findSuccessorMessage.toString());


		logger.info("ARE YOU HERE??!?!??!?!");

		if (!isInitialized || fingers[0].getChordNode() == thisNode) {
			//joinStep2(foundSuccessorMessage);
			//ChordNode senderNode = new ChordNode(foundSuccessorMessage.getSenderPeerID(), foundSuccessorMessage.getSenderHost());
			//ChordNode successorNode = new ChordNode(foundSuccessorMessage.getSuccessorPeerID(), foundSuccessorMessage.getSuccessorHost());
			ChordNode senderNode = new ChordNode(findSuccessorMessage.getKey(), findSuccessorMessage.getSender());

			//predecessorNode = senderNode;
			fingers[0].setChordNode(senderNode);

			//isInitialized = true;
			//triggerNotification(new DHTInitializedNotification());
			

			//NOW WE HAVE TO SEND TO THE OTHER NODE, THE REST
			logger.info("DONE INIT OF CONTACT MASTER SIDE");
			
			return;
		}
		
		if (Finger.belongsToSuccessor(thisNode.getPeerID(), fingers[0].getChordNode().getPeerID(), findSuccessorMessage.getKey())) {
			FoundSuccessorMessage foundSuccessorMessage = new FoundSuccessorMessage(findSuccessorMessage, thisNode, fingers[0].getChordNode());
			sendMessage(foundSuccessorMessage, foundSuccessorMessage.getOriginalSenderHost());
			return;
		}

		ChordNode closestPrecedingNode = closestPrecedingNode(findSuccessorMessage.getKey());
		FindSuccessorMessage findSuccessorMessage2 = new FindSuccessorMessage(findSuccessorMessage, thisNode.getHost());
		sendMessage(findSuccessorMessage2, closestPrecedingNode.getHost());
		pendingLookupRequests.remove(findSuccessorMessage2.getMid());
	}

	private void uponFoundSuccessorMessage(FoundSuccessorMessage foundSuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received FoundSuccessorMessage: " + foundSuccessorMessage.toString());

		if (!isInitialized) {
			joinStep2(foundSuccessorMessage);
			return;
		}
		if (fingersPendingSuccessor.containsKey(foundSuccessorMessage.getMid())) {
			updateFingerNode(foundSuccessorMessage);
			return;
		}

		LookupReply lookupReply = new LookupReply(foundSuccessorMessage);
		lookupReply.addElementToPeers(foundSuccessorMessage.getSenderPeerID().toByteArray(), foundSuccessorMessage.getSenderHost());
		sendReply(lookupReply, sourceProto);
	}

	private void uponGetPredecessorMessage(GetPredecessorMessage getPredecessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received GetPredecessorMessage: " + getPredecessorMessage.toString());

		ReturnPredecessorMessage returnPredecessorMessage = new ReturnPredecessorMessage(UUID.randomUUID(), PROTOCOL_ID, thisNode, predecessorNode);
		sendMessage(returnPredecessorMessage, fingers[0].getChordNode().getHost());
	}

	private void uponReturnPredecessorMessage(ReturnPredecessorMessage returnPredecessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received ReturnPredecessorMessage: " + returnPredecessorMessage.toString());

		if (Finger.belongsToOpenInterval(thisNode.getPeerID(), fingers[0].getChordNode().getPeerID(), returnPredecessorMessage.getPredecessorPeerID()) ||
				returnPredecessorMessage.getPredecessorPeerID().equals(returnPredecessorMessage.getSenderPeerID())) {
			fingers[0].setChordNode(new ChordNode(returnPredecessorMessage.getPredecessorPeerID(), returnPredecessorMessage.getPredecessor()));
		}

		NotifySuccessorMessage notifySuccessorMessage = new NotifySuccessorMessage(UUID.randomUUID(), PROTOCOL_ID, thisNode);
		sendMessage(notifySuccessorMessage, fingers[0].getChordNode().getHost());
	}

	private void uponNotifyPredecessorMessage(NotifySuccessorMessage notifySuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received UpdatePredecessorMessage: " + notifySuccessorMessage.toString());

		if (predecessorNode.getPeerID().equals(thisNode.getPeerID()) ||
				Finger.belongsToOpenInterval(predecessorNode.getPeerID(), thisNode.getPeerID(), notifySuccessorMessage.getSenderPeerID())) {
			predecessorNode = new ChordNode(notifySuccessorMessage.getSenderPeerID(), notifySuccessorMessage.getSender());
		}
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Timers ---------------------------------------- */

	private void retryTCPConnections(RetryTCPConnectionsTimer timer, long timerId) {
		logger.debug("retryTCPConnections: {}", pendingHostConnections);
		for (Host host : pendingHostConnections) {
			openConnection(host);
		}
	}

	private void stabilize(StabilizeTimer timer, long timerId) {
		logger.debug("stabilize: {}", timerId);

		GetPredecessorMessage getPredecessorMessage = new GetPredecessorMessage(UUID.randomUUID(), PROTOCOL_ID, thisNode);
		sendMessage(getPredecessorMessage, fingers[0].getChordNode().getHost());
	}

	private void fixFingers(FixFingersTimer timer, long timerId) {
		logger.debug("fixFingers: {}", timerId);

		int randomFingerIndex = ThreadLocalRandom.current().nextInt(1, fingers.length);
		UUID uuid = UUID.randomUUID();
		fingersPendingSuccessor.put(uuid, fingers[randomFingerIndex]);

		//TODO
		//FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(uuid, thisNode.getHost(), thisNode.getHost(), PROTOCOL_ID, fingers[randomFingerIndex].getStart());
		//uponFindSuccessorMessage(findSuccessorMessage, thisNode.getHost(), PROTOCOL_ID, tcpChannelId);
	}

	/* --------------------------------- TCPChannel Events ---------------------------- */

	//If a connection is successfully established, this event is triggered. In this protocol, we want to add the
	//respective peer to the membership, and inform the Dissemination protocol via a notification.
	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		Host peerHost = event.getNode();
		logger.debug("Connection to {} is up", peerHost);
		pendingHostConnections.remove(peerHost);

		logger.info("DO YOU GET HERE????");

		/* if (!isInitialized) {
			joinStep1(peerHost);
		}
 */
		FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), peerHost, thisNode.getHost(), thisNode.getPeerID());
		sendMessage(findSuccessorMessage, peerHost);
	}

	//If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
	//protocol. Alternatively, we could do smarter things like retrying the connection X times.
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down cause {}", peer, event.getCause());
	
		//logger.info("DO YOU GET HERE????");

		
	}

	//If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
	//pending set. Note that this event is only triggered while attempting a connection, not after connection.
	//Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
	}

	//If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
	//If we want to add the peer to the membership, we will establish our own outgoing connection.
	// (not the smartest protocol, but its simple)
	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		/* logger.trace("Connection from {} is up", event.getNode());

		logger.info("DO YOU GET HERE????");
		Host peerHost = event.getNode();
		logger.debug("Connection to {} is up", peerHost);
		pendingHostConnections.remove(peerHost);

		if (!isInitialized) {
			joinStep1(peerHost);
		} */
	}

	//A connection someone established to me is disconnected.
	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
	}

}
