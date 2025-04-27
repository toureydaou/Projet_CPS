package etape4.composants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import etape3.composants.AsynchronousNodeBCM;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape4.endpoints.CompositeMapContentManagementEndPoint;
import etape4.policies.IgnoreChordsPolicy;
import etape4.policies.ThreadsPolicy;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.components.pre.dcc.connectors.DynamicComponentCreationConnector;
import fr.sorbonne_u.components.pre.dcc.interfaces.DynamicComponentCreationCI;
import fr.sorbonne_u.components.pre.dcc.ports.DynamicComponentCreationOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * La classe <code>DynamicNodeBCM</code> représente un composant de nœud
 * dans un système distribué qui gère l'accès au contenu et les opérations
 * MapReduce de manière asynchrone. Elle implémente les interfaces
 * <code>ContentAccessI</code> et <code>ParallelMapReduceI</code>. 
 * Ce composant implémente également la gestion du noeud via l'interface
 * <code>DHTManagementI</code>.
 * 
 * <p>
 * Elle gère quatre types principaux d'opérations :
 * <ul>
 * <li>Accès au contenu (GET, PUT, REMOVE) pour stocker et récupérer des données
 * depuis la DHT.</li>
 * <li>Opérations MapReduce (MAP, REDUCE) pour traiter des données en utilisant
 * le paradigme MapReduce.</li>
 * <li>Nettoyage des calculs pour effacer les données précédemment
 * stockées.</li>
 * <li> Opérations SPLIT, MERGE et computeChords pour calculer les cordes du noeud
 * </li>
 * </ul>
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@RequiredInterfaces(required = { DHTManagementCI.class, ParallelMapReduceCI.class, ContentAccessCI.class,
		ResultReceptionCI.class, MapReduceResultReceptionCI.class, DynamicComponentCreationCI.class })
@OfferedInterfaces(offered = { DHTManagementCI.class, ParallelMapReduceCI.class, ContentAccessCI.class,
		MapReduceResultReceptionCI.class })
public class DynamicNodeBCM extends AsynchronousNodeBCM
		implements DHTManagementI, ParallelMapReduceI, MapReduceResultReceptionI {
	
	private static final String CONTENT_ACCESS_HANDLER_URI = "Content-Access-Pool-Threads";
	
	private static final String MAP_REDUCE_HANDLER_URI = "Map-Reduce-Pool-Threads";
	
	private static final String DHT_MANAGEMENT_HANDLER_URI = "Dht-Management-Pool-Threads";
	
	private static final String MAP_REDUCE_RESULT_RECEPTION_HANDLER_URI = "Result-Reception-Map-Reduce-Pool-Threads";

	protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound;

	protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointInbound;

	protected MapReduceResultReceptionEndPoint mapReduceResultReceptionEndPoint;

	protected ArrayList<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> chords;

	protected DynamicComponentCreationOutboundPort porttoNewNode;

	protected String jvmUri;

	private static final String NOUVEAU_NOEUD_URI = "Noeud-créé-apès-un-split";

	private HashMap<String, ArrayList<CompletableFuture<Serializable>>> resultsMapReduce;

	protected final ReentrantReadWriteLock mapReduceLock;

	protected final ReentrantReadWriteLock splitLock;
	
	protected ConcurrentHashMap<String, Integer> computationIndices = new ConcurrentHashMap<>();

	int indice;

	/**
	 * Crée le noeud dynamique
	 * 
	 * @param jvmUri
	 * @param uri
	 * @param compositeMapContentManagementEndPointInbound
	 * @param compositeMapContentManagementEndPointOutbound
	 * @param intervalle
	 * @throws ConnectionException
	 */
	protected DynamicNodeBCM(String jvmUri, String uri,
			CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointInbound,
			CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound, IntInterval intervalle)
			throws ConnectionException {
		super(uri, intervalle);
		this.jvmUri = jvmUri;
		this.compositeMapContentManagementEndPointOutbound = compositeMapContentManagementEndPointOutbound;
		this.compositeMapContentManagementEndPointInbound = compositeMapContentManagementEndPointInbound;
		this.mapReduceResultReceptionEndPoint = new MapReduceResultReceptionEndPoint();
		this.resultsMapReduce = new HashMap<String, ArrayList<CompletableFuture<Serializable>>>();
		this.mapReduceLock = new ReentrantReadWriteLock();
		this.splitLock = new ReentrantReadWriteLock();
		
		this.compositeMapContentManagementEndPointInbound.setExecutorServiceIndexContentAccessService(
				this.createNewExecutorService(URIGenerator.generateURI(CONTENT_ACCESS_HANDLER_URI),
						ThreadsPolicy.NUMBER_CONTENT_ACCESS_THREADS, true));
		
		this.compositeMapContentManagementEndPointInbound.setExecutorServiceIndexMapReduceService(
				this.createNewExecutorService(URIGenerator.generateURI(MAP_REDUCE_HANDLER_URI),
						ThreadsPolicy.NUMBER_MAP_REDUCE_THREADS, true));
		
		this.compositeMapContentManagementEndPointInbound.setExecutorServiceIndexDHTManagementService(
				this.createNewExecutorService(URIGenerator.generateURI(DHT_MANAGEMENT_HANDLER_URI),
						ThreadsPolicy.NUMBER_DHT_MANAGEMEMENT_THREADS, true));
		
		this.mapReduceResultReceptionEndPoint.setExecutorIndex(
				this.createNewExecutorService(URIGenerator.generateURI(MAP_REDUCE_RESULT_RECEPTION_HANDLER_URI),
						ThreadsPolicy.NUMBER_ACCEPT_RESULT_MAP_REDUCE_THREADS, true));
		
		this.mapReduceResultReceptionEndPoint.initialiseServerSide(this);
		this.compositeMapContentManagementEndPointInbound.initialiseServerSide(this);
	}

	/**
	 * Classe interne qui permet de transmettre le contenu du noeud à un autre
	 */
	protected class NodeContent implements NodeContentI {

		private static final long serialVersionUID = 1L;

		public ConcurrentHashMap<Integer, ContentDataI> content;

		public IntInterval intervalle;
		
		public String nodeUri;

		protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound;

		protected NodeContent(ConcurrentHashMap<Integer, ContentDataI> content, IntInterval intervalle,
				CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound, String nodeUri) {
			this.content = content;
			this.intervalle = intervalle;
			this.nodeUri = nodeUri;
			this.compositeMapContentManagementEndPointOutbound = compositeMapContentManagementEndPointOutbound;
		}
	}

	/**
	 * Classe interne qui permet de vérifier l'état d'un noeud afin de connaitre sa charge actuelle 
	 */
	public class NodeState implements NodeStateI {

		private static final long serialVersionUID = 1L;

		protected int contentDataSize;

		public NodeState(int contentDataSize) {
			this.contentDataSize = contentDataSize;
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#initialiseContent(fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI.NodeContentI)
	 */
	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		this.content.putAll(((NodeContent) content).content);
		this.intervalle = ((NodeContent) content).intervalle.clone();
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#getCurrentState()
	 */
	@Override
	public NodeStateI getCurrentState() throws Exception {
		return new NodeState(this.content.size());
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#suppressNode()
	 */
	@Override
	public NodeContentI suppressNode() throws Exception {
		NodeContent nodeContent = new NodeContent(this.content, this.intervalle,
				(CompositeMapContentManagementEndPoint) this.compositeMapContentManagementEndPointOutbound
						.copyWithSharable(), this.uri);

		this.finalise();
		return nodeContent;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#split(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {

		
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.add(computationURI);
			System.out.println("Reception de la requete 'SPLIT' sur le noeud " + this.uri);

			if (loadPolicy.shouldSplitInTwoAdjacentNodes(content.size())) {

				System.out.println("Fission du noeud " + this.uri);

				assert this.porttoNewNode != null;
				assert this.porttoNewNode.connected();

				CompositeMapContentManagementEndPoint nouvelEndpointEntreNoeuds = new CompositeMapContentManagementEndPoint();

				String nouveauNoeudURI = this.porttoNewNode.createComponent(DynamicNodeBCM.class.getCanonicalName(),
						new Object[] { this.jvmUri, NOUVEAU_NOEUD_URI,
								(CompositeMapContentManagementEndPoint) nouvelEndpointEntreNoeuds.copyWithSharable(),
								(CompositeMapContentManagementEndPoint) this.compositeMapContentManagementEndPointOutbound
										.copyWithSharable(),
								new IntInterval(-1, 0) });
				this.compositeMapContentManagementEndPointOutbound.cleanUpClientSide();
				this.compositeMapContentManagementEndPointOutbound = (CompositeMapContentManagementEndPoint) nouvelEndpointEntreNoeuds
						.copyWithSharable();

				this.compositeMapContentManagementEndPointOutbound.initialiseClientSide(this);

				ConcurrentHashMap<Integer, ContentDataI> firstPart = new ConcurrentHashMap<>();
				ConcurrentHashMap<Integer, ContentDataI> secondPart = new ConcurrentHashMap<>();

				double f = this.intervalle.first();
				double l = this.intervalle.last();
				int mid = (int) ((f + l) / 2.0);

				for (Map.Entry<Integer, ContentDataI> entry : this.content.entrySet()) {
					if (entry.getKey() < mid + 1) {
						firstPart.put(entry.getKey(), entry.getValue());
					} else {
						secondPart.put(entry.getKey(), entry.getValue());
					}
				}

				this.content = firstPart;
				NodeContent contenuSplit = new NodeContent(secondPart, intervalle.split(), null, this.uri);

				this.porttoNewNode.startComponent(nouveauNoeudURI);

				this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
						.initialiseContent(contenuSplit);

			}
			this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
					.split(computationURI, loadPolicy, caller);

		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#merge(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public  <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {

		
		System.out.println("Reception de la requete 'MERGE' sur le noeud " + this.uri);
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.add(computationURI);
			
			
			if (!(this.getChordInfo(1).second() < this.intervalle.first())) {
				NodeState stateSuivant = (NodeState) this.compositeMapContentManagementEndPointOutbound
						.getDHTManagementEndpoint().getClientSideReference().getCurrentState();

				if (loadPolicy.shouldMergeWithNextNode(content.size(), stateSuivant.contentDataSize)) {

					NodeContent contentSuivant = (NodeContent) this.compositeMapContentManagementEndPointOutbound
							.getDHTManagementEndpoint().getClientSideReference().suppressNode();

					System.out.println("Fusion du noeud: " + this.uri + " et du noeud: " + contentSuivant.nodeUri );
					this.content.putAll(contentSuivant.content);

					this.intervalle.merge(contentSuivant.intervalle);
					
					this.compositeMapContentManagementEndPointOutbound.cleanUpClientSide();

					this.compositeMapContentManagementEndPointOutbound = (CompositeMapContentManagementEndPoint) contentSuivant.compositeMapContentManagementEndPointOutbound
							.copyWithSharable();

					this.compositeMapContentManagementEndPointOutbound.initialiseClientSide(this);

				}
			}
			
			this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
					.merge(computationURI, loadPolicy, caller);

		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#computeChords(java.lang.String, int)
	 */
	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.add(computationURI);
			this.chords.clear();
			int offset = 1;
			for (int i = 0; i < numberOfChords; i++) {
				SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> tmp_info = this
						.getChordInfo(offset);
				if (tmp_info.second() >= this.intervalle.first()) {
					this.chords.add(tmp_info);
				}
				offset = offset * 2;
			}
			this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
					.computeChords(computationURI, numberOfChords);
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI#getChordInfo(int)
	 */
	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		if (offset > 0) {
			return this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint()
					.getClientSideReference().getChordInfo(offset - 1);
		}
		return new SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>(
				(CompositeMapContentManagementEndPoint) this.compositeMapContentManagementEndPointInbound
						.copyWithSharable(),
				this.intervalle.first());
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#get(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'GET' sur le noeud " + this.uri);
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {

				this.hashMapLock.readLock().lock();
				try {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					caller.getClientSideReference().acceptResult(computationURI, content.get(key.hashCode()));
					caller.cleanUpClientSide();
				} finally {
					this.hashMapLock.readLock().unlock();
				}
			} else {
				if (chords.isEmpty()) {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					System.out.println("Envoi du résultat du 'GET' sur la facade depuis le noeud " + this.uri);
					// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
					// la DHT
					caller.getClientSideReference().acceptResult(computationURI, null);
					caller.cleanUpClientSide();
				} else {
					int gap = Integer.MAX_VALUE;
					ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> next_endpoint = chords
							.get(0).first();
					for (SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord : chords) {
						int min_gap = key.hashCode() - chord.second();
						if (min_gap < gap && min_gap >= 0) {
							gap = min_gap;
							next_endpoint = (ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>) chord
									.first().copyWithSharable();
						}
					}

					if (!next_endpoint.clientSideInitialised()) {
						next_endpoint.initialiseClientSide(this);
					}

					next_endpoint.getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller);

					next_endpoint.cleanUpClientSide();
				}
			}
		}

	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#put(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		System.out.println(
				"Reception de la requete 'PUT' le noeud " + this.uri + " identifiant requete : " + computationURI);

		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				this.hashMapLock.writeLock().lock();
				try {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					ContentDataI oldValue = content.put(key.hashCode(), value);
					caller.getClientSideReference().acceptResult(computationURI, oldValue);
					caller.cleanUpClientSide();
				} finally {
					this.hashMapLock.writeLock().unlock();
				}
			} else {

				if (chords.isEmpty()) {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					System.out.println("Envoi du résultat du 'PUT' sur la facade depuis le noeud " + this.uri);
					// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
					// la DHT
					caller.getClientSideReference().acceptResult(computationURI, null);
					caller.cleanUpClientSide();
				} else {
					int gap = Integer.MAX_VALUE;
					ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> next_endpoint = chords
							.get(0).first();
					for (SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord : chords) {
						int min_gap = key.hashCode() - chord.second();
						if (min_gap < gap && min_gap > 0) {
							gap = min_gap;
							next_endpoint = chord.first();
						}
					}

					if (!next_endpoint.clientSideInitialised()) {
						next_endpoint.initialiseClientSide(this);
					}

					next_endpoint.getContentAccessEndpoint().getClientSideReference().put(computationURI, key, value,
							caller);
					next_endpoint.cleanUpClientSide();

				}
			}
		}
		
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#remove(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println(
				"Reception de la requete 'REMOVE' le noeud " + this.uri + " identifiant requete : " + computationURI);
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				this.hashMapLock.writeLock().lock();
				try {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					ContentDataI oldValue = content.remove(key.hashCode());
					caller.getClientSideReference().acceptResult(computationURI, oldValue);
					caller.cleanUpClientSide();
				} finally {
					this.hashMapLock.writeLock().unlock();
				}
			} else {

				if (chords.isEmpty()) {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					System.out.println("Envoi du résultat du 'REMOVE' sur la facade depuis le noeud " + this.uri);
					// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
					// la DHT
					caller.getClientSideReference().acceptResult(computationURI, null);
					caller.cleanUpClientSide();
				} else {
					int gap = Integer.MAX_VALUE;
					ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> next_endpoint = chords
							.get(0).first();
					for (SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> chord : chords) {
						int min_gap = key.hashCode() - chord.second();
						if (min_gap < gap && min_gap > 0) {
							gap = min_gap;
							next_endpoint = chord.first();
						}
					}

					if (!next_endpoint.clientSideInitialised()) {
						next_endpoint.initialiseClientSide(this);
					}

					next_endpoint.getContentAccessEndpoint().getClientSideReference().remove(computationURI, key,
							caller);

					next_endpoint.cleanUpClientSide();

				}
			}
		}
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#clearComputation(java.lang.String)
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.remove(computationURI);
		} else {
			this.compositeMapContentManagementEndPointOutbound.getContentAccessEndpoint().getClientSideReference()
					.clearComputation(computationURI);
		}
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#clearMapReduceComputation(java.lang.String)
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (listeUriMapOperations.contains(computationURI) && listeUriReduceOperations.contains(computationURI)) {
			this.listeUriMapOperations.remove(computationURI);
			this.listeUriReduceOperations.remove(computationURI);
			this.memory.remove(computationURI);
		} else {
			this.compositeMapContentManagementEndPointOutbound.getMapReduceEndpoint().getClientSideReference()
					.clearMapReduceComputation(computationURI);
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI#parallelMap(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {

		System.out.println("Reception de la requete 'MAP REDUCE' (MAP) sur le noeud " + this.uri
				+ " identifiant requete : " + computationURI);
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.addIfAbsent(computationURI);

			// Vérification du type de politique
			if (!(parallelismPolicy instanceof IgnoreChordsPolicy)) {
				throw new IllegalArgumentException("Unsupported parallelism policy");
			}
			IgnoreChordsPolicy policy = (IgnoreChordsPolicy) parallelismPolicy;

			int debut = Math.min(chords.size(), policy.getNbreChordsIgnores());

			ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> next_endpoint;

			indice = 0;

			int increment = 1;

			for (int i = debut; i < chords.size(); i++) {
				next_endpoint = chords.get(i).first();
				// Envoie la tâche au chord

				this.mapReduceLock.writeLock().lock();
				if (!next_endpoint.clientSideInitialised()) {
					next_endpoint.initialiseClientSide(this);
				}

				next_endpoint.getMapReduceEndpoint().getClientSideReference().parallelMap(computationURI, selector,
						processor,
						// Réduit la profondeur max pour éviter les boucles infinies
						new IgnoreChordsPolicy(policy.getNbreChordsIgnores() + increment));

				increment++;
				next_endpoint.cleanUpClientSide();
				this.mapReduceLock.writeLock().unlock();

			}

			// Calcul dans le noeud
			this.hashMapLock.readLock().lock();
			try {

				CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
				memory.putIfAbsent(computationURI, futureStream);
				memory.get(computationURI)
						.complete((Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));

			} finally {
				this.hashMapLock.readLock().unlock();
			}

		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI#parallelReduce(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A, A, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {

		System.out.println("Reception de la requete 'MAP REDUCE' (REDUCE) sur le noeud " + this.uri
				+ " identifiant requete : " + computationURI);
		if (!listeUriReduceOperations.contains(computationURI)) {
			listeUriReduceOperations.add(computationURI);

			CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
			memory.putIfAbsent(computationURI, futureStream);

			Stream<ContentDataI> localStream = memory.get(computationURI).get();

			@SuppressWarnings("unchecked")
			A localReduce = localStream.reduce(identityAcc, (u, d) -> reductor.apply(u, (R) d), combinator);
			localReduce = combinator.apply(currentAcc, localReduce);

			if (!(parallelismPolicy instanceof IgnoreChordsPolicy)) {
				throw new IllegalArgumentException("Unsupported parallelism policy");
			}
			IgnoreChordsPolicy policy = (IgnoreChordsPolicy) parallelismPolicy;

			int debut = Math.min(chords.size(), policy.getNbreChordsIgnores());

			ArrayList<CompletableFuture<Serializable>> listeResults = new ArrayList<CompletableFuture<Serializable>>();

			resultsMapReduce.put(computationURI, listeResults);

			ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> next_endpoint;

			int increment = 1;

			this.mapReduceLock.writeLock().lock();
            computationIndices.put(computationURI, 0);

			for (int i = debut; i < chords.size(); i++) {

				resultsMapReduce.get(computationURI).add(new CompletableFuture<Serializable>());
				next_endpoint = chords.get(i).first();
				// Envoie la tâche au chord

				if (!next_endpoint.clientSideInitialised()) {
					next_endpoint.initialiseClientSide(this);
				}

				next_endpoint.getMapReduceEndpoint().getClientSideReference().parallelReduce(computationURI, reductor,
						combinator, identityAcc, identityAcc,
						new IgnoreChordsPolicy(policy.getNbreChordsIgnores() + increment),
						this.mapReduceResultReceptionEndPoint);
				increment++;
				next_endpoint.cleanUpClientSide();

			}
			this.mapReduceLock.writeLock().unlock();

			for (CompletableFuture<Serializable> result : resultsMapReduce.get(computationURI)) {
				@SuppressWarnings("unchecked")
				A resTemporaire = (A) result.get();
				localReduce = combinator.apply(resTemporaire, localReduce);
			}

			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}

			System.out.println("Envoi du résultat du 'MAP REDUCE' depuis le noeud " + this.uri);

			caller.getClientSideReference().acceptResult(computationURI, "nom du noeud qui envoie", localReduce);
			
			caller.cleanUpClientSide();

		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI#acceptResult(java.lang.String, java.lang.String, java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
	    // Atomically get and increment the index for this specific computation
	    int currentIndex = computationIndices.computeIfAbsent(computationURI, k -> 0);
	    
	    // Update the results and increment the index atomically
	    resultsMapReduce.get(computationURI).get(currentIndex).complete(acc);
	    
	    // Update the index for the next result
	    computationIndices.put(computationURI, currentIndex + 1);
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#start()
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.startOrigin();
		try {
			if (!this.compositeMapContentManagementEndPointOutbound.clientSideInitialised()) {
				this.compositeMapContentManagementEndPointOutbound.initialiseClientSide(this);
			}
			this.porttoNewNode = new DynamicComponentCreationOutboundPort(this);
			this.porttoNewNode.localPublishPort();
			this.doPortConnection(this.porttoNewNode.getPortURI(), this.jvmUri + AbstractCVM.DCC_INBOUNDPORT_URI_SUFFIX,
					DynamicComponentCreationConnector.class.getCanonicalName());
			this.chords = new ArrayList<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>>();

			AbstractComponent.checkImplementationInvariant(this);
			AbstractComponent.checkInvariant(this);
		} catch (Exception e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#finalise()
	 */
	@Override
	public void finalise() throws Exception {
		if (!this.isFinalised()) {
			this.logMessage("stopping node component.");
			
			if (!this.compositeMapContentManagementEndPointOutbound.clientSideClean()) {
				this.compositeMapContentManagementEndPointOutbound.cleanUpClientSide();
			}

			if (this.porttoNewNode.connected()) {
				this.doPortDisconnection(this.porttoNewNode.getPortURI());
			}

			super.finaliseOrigin();
		}
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#shutdown()
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {

		try {
		
			this.compositeMapContentManagementEndPointInbound.cleanUpServerSide();
			
		
			this.mapReduceResultReceptionEndPoint.cleanUpServerSide();
			if (this.porttoNewNode.isPublished()) {
				
				this.porttoNewNode.unpublishPort();
				
			}
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownOrigin();
	}

	/**
	 * @see etape3.composants.AsynchronousNodeBCM#shutdownNow()
	 */
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.compositeMapContentManagementEndPointInbound.cleanUpServerSide();
			this.mapReduceResultReceptionEndPoint.cleanUpServerSide();
			if (this.porttoNewNode.isPublished()) {
				this.porttoNewNode.unpublishPort();
			}
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNowOrigin();
	}

}
