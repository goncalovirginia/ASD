package protocols.dht.chord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.AutomatedApp;
import protocols.dht.chord.messages.*;
import protocols.dht.chord.notifications.DHTInitializedNotification;
import protocols.dht.chord.notifications.PeerDownNotification;
import protocols.dht.chord.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord.replies.LookupReply;
import protocols.dht.chord.requests.LookupRequest;
import protocols.dht.chord.timers.FixFingersTimer;
import protocols.dht.chord.timers.RetryLookupsTimer;
import protocols.dht.chord.timers.StabilizeTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class ChordDHT extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(ChordDHT.class);

	public static final short PROTOCOL_ID = 500;
	public static final String PROTOCOL_NAME = "ChordDHT";

	private final short COMM_PROTOCOL_ID;

	private final int tcpChannelId;

	private ChordNode predecessorNode, successorSuccessorNode;
	private final ChordNode thisNode;
	private final Finger[] fingers;

	private final Map<UUID, FindSuccessorMessage> lookupsPendingResponse;
	private final Map<UUID, Finger> fixFingersPendingResponse;

	private boolean isInitialized;

	public ChordDHT(Properties properties, Host thisHost, short commProtocolID) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);

		COMM_PROTOCOL_ID = commProtocolID;

		lookupsPendingResponse = new HashMap<>();
		fixFingersPendingResponse = new HashMap<>();
		isInitialized = false;

		//initialize thisNode and predecessorNode
		String myPeerIDHex = properties.getProperty(AutomatedApp.PROPERTY_NODE_ID);
		BigInteger myPeerID = new BigInteger(1, new BigInteger(myPeerIDHex, 16).toByteArray());
		thisNode = new ChordNode(myPeerID, thisHost);
		predecessorNode = thisNode;
		successorSuccessorNode = thisNode;

		//initialize fingers
		int numFingers = Integer.parseInt(properties.getProperty("id_bits"));
		fingers = new Finger[numFingers];
		BigInteger fingerEnd = thisNode.getPeerID().add(BigInteger.TWO.pow(fingers.length)).mod(BigInteger.TWO.pow(fingers.length));
		for (int i = fingers.length - 1; i >= 0; i--) {
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
		registerMessageHandler(tcpChannelId, NotifySuccessorMessage.MSG_ID, this::uponNotifySuccessorMessage, this::uponMessageFail);

		//register timer handlers
		registerTimerHandler(RetryLookupsTimer.TIMER_ID, this::retrySendMessages);
		registerTimerHandler(StabilizeTimer.TIMER_ID, this::stabilize);
		registerTimerHandler(FixFingersTimer.TIMER_ID, this::fixFingers);
	}

	@Override
	public void init(Properties props) {
		//inform the point2point algorithm above about the TCP channel to use
		triggerNotification(new TCPChannelCreatedNotification(tcpChannelId));

		setupPeriodicTimer(new RetryLookupsTimer(), 3000, 3000);
		setupPeriodicTimer(new StabilizeTimer(), 1000, 3000);
		setupPeriodicTimer(new FixFingersTimer(), 3000, 3000);

		//establish TCP connection to contact host
		if (props.containsKey("contact")) {
			connectToHost(props.getProperty("contact"));
		}
	}

	private void connectToHost(String contact) {
		try {
			String[] hostElems = contact.split(":");
			Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
			FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(UUID.randomUUID(), thisNode.getHost(), thisNode.getHost(), thisNode.getPeerID());
			openConnectionAndSendMessage(findSuccessorMessage, contactHost);
		} catch (Exception e) {
			logger.error("Invalid contact on configuration: {}", contact);
			logger.error(e.getStackTrace());
			System.exit(-1);
		}
	}

	private void openConnectionAndSendMessage(ProtoMessage protoMessage, Host host) {
		openConnection(host);
		sendMessage(protoMessage, host);
	}

	private void setIsInitialized(boolean isInitialized) {
		logger.info("Initialization state updated: {} | Direct Peers: {} -> [{}] -> {}", isInitialized, predecessorNode.getHost(), thisNode.getHost(), fingers[0].getChordNode().getHost());
		triggerNotification(new DHTInitializedNotification(this.isInitialized = isInitialized));
	}

	private ChordNode closestPrecedingNode(BigInteger peerID) {
		for (int i = fingers.length - 1; i >= 0; i--) {
			if (Finger.belongsToOpenInterval(thisNode.getPeerID(), peerID, fingers[i].getChordNode().getPeerID())) {
				return fingers[i].getChordNode();
			}
		}
		return thisNode;
	}

	private void fixFinger(FoundSuccessorMessage foundSuccessorMessage) {
		ChordNode newSuccessorNode = new ChordNode(foundSuccessorMessage.getSuccessorPeerID(), foundSuccessorMessage.getSuccessorHost());
		fixFingersPendingResponse.remove(foundSuccessorMessage.getMid()).setChordNode(newSuccessorNode);
	}

	private void fixFingersFromDisconnectingNode(Host disconnectingHost) {
		for (Finger finger : fingers) {
			if (finger.getChordNode().getHost().equals(disconnectingHost)) finger.setChordNode(thisNode);
		}
		//if the disconnectingHost was thisNode's successor, replace it with the successor's successor
		if (fingers[0].getChordNode().getHost().equals(thisNode.getHost())) {
			fingers[0].setChordNode(successorSuccessorNode);
		}
		//check the successor again - if it's still pointing to thisNode, it means the node is alone in the network, and should be paused again
		if (fingers[0].getChordNode().getHost().equals(thisNode.getHost())) {
			predecessorNode = thisNode;
			setIsInitialized(false);
		}
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponLookupRequest(LookupRequest request, short protoID) {
		logger.info("Received LookupRequest: {}", new BigInteger(1, request.getPeerID()));

		FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(request.getMid(), thisNode.getHost(), thisNode.getHost(), new BigInteger(1, request.getPeerID()));
		lookupsPendingResponse.put(findSuccessorMessage.getMid(), findSuccessorMessage);
		uponFindSuccessorMessage(findSuccessorMessage, thisNode.getHost(), protoID, tcpChannelId);
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponFindSuccessorMessage(FindSuccessorMessage findSuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received FindSuccessorMessage: {} - {}", findSuccessorMessage.getOriginalSender(), findSuccessorMessage.getKey());

		if (!isInitialized || Finger.belongsToSuccessor(thisNode.getPeerID(), fingers[0].getChordNode().getPeerID(), findSuccessorMessage.getKey())) {
			FoundSuccessorMessage foundSuccessorMessage = new FoundSuccessorMessage(findSuccessorMessage, thisNode, fingers[0].getChordNode());
			openConnectionAndSendMessage(foundSuccessorMessage, foundSuccessorMessage.getOriginalSenderHost());
			return;
		}
		//optimization for when the searched key is between predecessorNode and thisNode, avoids going around the whole ring
		if (Finger.belongsToSuccessor(predecessorNode.getPeerID(), thisNode.getPeerID(), findSuccessorMessage.getKey())) {
			FoundSuccessorMessage foundSuccessorMessage = new FoundSuccessorMessage(findSuccessorMessage, predecessorNode, thisNode);
			if (findSuccessorMessage.getOriginalSender().equals(thisNode.getHost())) {
				uponFoundSuccessorMessage(foundSuccessorMessage, thisNode.getHost(), PROTOCOL_ID, channelId);
				return;
			}
			openConnectionAndSendMessage(foundSuccessorMessage, foundSuccessorMessage.getOriginalSenderHost());
			return;
		}

		ChordNode closestPrecedingNode = closestPrecedingNode(findSuccessorMessage.getKey());
		FindSuccessorMessage findSuccessorMessage2 = new FindSuccessorMessage(findSuccessorMessage, thisNode.getHost());
		openConnectionAndSendMessage(findSuccessorMessage2, closestPrecedingNode.getHost());
	}

	private void uponFoundSuccessorMessage(FoundSuccessorMessage foundSuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received FoundSuccessorMessage: {}", foundSuccessorMessage.toString());

		//used for the network's second and subsequent nodes, on their first response from the first node
		if (!isInitialized) {
			predecessorNode = new ChordNode(foundSuccessorMessage.getSenderPeerID(), foundSuccessorMessage.getSenderHost());
			fingers[0].setChordNode(new ChordNode(foundSuccessorMessage.getSuccessorPeerID(), foundSuccessorMessage.getSuccessorHost()));
			setIsInitialized(true);
			return;
		}
		//used for fix fingers responses
		if (fixFingersPendingResponse.containsKey(foundSuccessorMessage.getMid())) {
			fixFinger(foundSuccessorMessage);
			return;
		}

		LookupReply lookupReply = new LookupReply(foundSuccessorMessage);

		if (foundSuccessorMessage.getSenderPeerID().equals(thisNode.getPeerID()))
			lookupReply.addElementToPeers(predecessorNode.getPeerID(), predecessorNode.getHost());
		else
			lookupReply.addElementToPeers(foundSuccessorMessage.getSenderPeerID(), foundSuccessorMessage.getSenderHost());

		lookupReply.addElementToPeers(foundSuccessorMessage.getSuccessorPeerID(), foundSuccessorMessage.getSuccessorHost());

		sendReply(lookupReply, COMM_PROTOCOL_ID);
		lookupsPendingResponse.remove(foundSuccessorMessage.getMid());
	}

	private void uponGetPredecessorMessage(GetPredecessorMessage getPredecessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received GetPredecessorMessage: {}", getPredecessorMessage.toString());

		ReturnPredecessorMessage returnPredecessorMessage = new ReturnPredecessorMessage(getPredecessorMessage.getMid(), predecessorNode, thisNode, fingers[0].getChordNode());
		openConnectionAndSendMessage(returnPredecessorMessage, getPredecessorMessage.getSender());
	}

	private void uponReturnPredecessorMessage(ReturnPredecessorMessage returnPredecessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received ReturnPredecessorMessage: {}", returnPredecessorMessage.toString());

		if (Finger.belongsToOpenInterval(thisNode.getPeerID(), fingers[0].getChordNode().getPeerID(), returnPredecessorMessage.getPredecessorPeerID())) {
			logger.info("Updated successor: {} -> {}", fingers[0].getChordNode().getHost(), returnPredecessorMessage.getPredecessor());
			fingers[0].setChordNode(new ChordNode(returnPredecessorMessage.getPredecessorPeerID(), returnPredecessorMessage.getPredecessor()));
		}

		successorSuccessorNode = new ChordNode(returnPredecessorMessage.getSuccessorPeerID(), returnPredecessorMessage.getSuccessor());
		NotifySuccessorMessage notifySuccessorMessage = new NotifySuccessorMessage(UUID.randomUUID(), thisNode);
		openConnectionAndSendMessage(notifySuccessorMessage, fingers[0].getChordNode().getHost());
	}

	private void uponNotifySuccessorMessage(NotifySuccessorMessage notifySuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received UpdatePredecessorMessage: {}", notifySuccessorMessage.toString());

		if (!isInitialized || Finger.belongsToOpenInterval(predecessorNode.getPeerID(), thisNode.getPeerID(), notifySuccessorMessage.getSenderPeerID())) {
			logger.info("Updated predecessor: {} -> {}", predecessorNode.getHost(), notifySuccessorMessage.getSender());
			predecessorNode = new ChordNode(notifySuccessorMessage.getSenderPeerID(), notifySuccessorMessage.getSender());

			if (!isInitialized) {
				fingers[0].setChordNode(predecessorNode);
				setIsInitialized(true);
			}
		}
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Timers ---------------------------------------- */

	private void retrySendMessages(RetryLookupsTimer timer, long timerId) {
		logger.debug("RetryLookupsTimer: {}", lookupsPendingResponse);

		for (FindSuccessorMessage findSuccessorMessage : lookupsPendingResponse.values()) {
			uponFindSuccessorMessage(findSuccessorMessage, findSuccessorMessage.getOriginalSender(), PROTOCOL_ID, tcpChannelId);
		}
	}

	private void stabilize(StabilizeTimer timer, long timerId) {
		logger.debug("stabilize: {}", timerId);

		if (!isInitialized) return;

		GetPredecessorMessage getPredecessorMessage = new GetPredecessorMessage(UUID.randomUUID(), thisNode);
		openConnectionAndSendMessage(getPredecessorMessage, fingers[0].getChordNode().getHost());
	}

	private void fixFingers(FixFingersTimer timer, long timerId) {
		logger.debug("fixFingers: {}", timerId);

		if (!isInitialized) return;

		int randomFingerIndex = ThreadLocalRandom.current().nextInt(1, fingers.length);
		UUID uuid = UUID.randomUUID();
		fixFingersPendingResponse.put(uuid, fingers[randomFingerIndex]);

		FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(uuid, thisNode.getHost(), thisNode.getHost(), fingers[randomFingerIndex].getStart());
		uponFindSuccessorMessage(findSuccessorMessage, thisNode.getHost(), PROTOCOL_ID, tcpChannelId);
	}

	/* --------------------------------- TCPChannel Events ---------------------------- */

	//triggered when an outgoing connection is up
	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		Host peerHost = event.getNode();
		logger.debug("Connection to {} is up", peerHost);
	}

	//triggered when an outgoing connection is down
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down, cause: {}", peer, event.getCause());

		fixFingersFromDisconnectingNode(peer);
		triggerNotification(new PeerDownNotification(peer));
	}

	//triggered when an outgoing connection fails to be established
	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
	}

	//triggered when an incoming connection is up
	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		logger.trace("Connection from {} is up", event.getNode());
	}

	//triggered when an incoming connection is down
	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
	}

}
