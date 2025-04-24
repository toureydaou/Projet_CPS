package etape4.composants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import etape3.composants.AsynchronousNodeBCM;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape4.endpoints.CompositeMapContentManagementEndPoint;
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

@RequiredInterfaces(required = { DHTManagementCI.class, ParallelMapReduceCI.class, ContentAccessCI.class,
		ResultReceptionCI.class, MapReduceResultReceptionCI.class, DynamicComponentCreationCI.class })
@OfferedInterfaces(offered = { DHTManagementCI.class, ParallelMapReduceCI.class, ContentAccessCI.class,
		MapReduceResultReceptionCI.class })
public class DynamicNodeBCM extends AsynchronousNodeBCM
		implements DHTManagementI, ParallelMapReduceI, MapReduceResultReceptionI {

	protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound;

	protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointInbound;

	protected MapReduceResultReceptionEndPoint mapReduceResultReceptionEndPoint;

	protected ArrayList<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> chords;

	protected DynamicComponentCreationOutboundPort porttoNewNode;

	protected String jvmUri;

	private static final String NOUVEAU_NOEUD_URI = "Noeud-créé-apès-un-split";

	private HashMap<String, ArrayList<CompletableFuture<Serializable>>> resultsMapReduce;

	int indice;

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

		this.mapReduceResultReceptionEndPoint.initialiseServerSide(this);
		this.compositeMapContentManagementEndPointInbound.initialiseServerSide(this);
	}

	protected class NodeContent implements NodeContentI {

		private static final long serialVersionUID = 1L;

		public ConcurrentHashMap<Integer, ContentDataI> content;

		public IntInterval intervalle;

		protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound;

		protected NodeContent(ConcurrentHashMap<Integer, ContentDataI> content, IntInterval intervalle,
				CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound) {
			this.content = content;
			this.intervalle = intervalle;
			this.compositeMapContentManagementEndPointOutbound = compositeMapContentManagementEndPointOutbound;
		}

	}

	public class NodeState implements NodeStateI {

		private static final long serialVersionUID = 1L;

		protected int contentDataSize;

		public NodeState(int contentDataSize) {
			this.contentDataSize = contentDataSize;
		}
	}

	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		this.content.putAll(((NodeContent) content).content);
		this.intervalle = ((NodeContent) content).intervalle.clone();
	}

	@Override
	public NodeStateI getCurrentState() throws Exception {
		return new NodeState(this.content.size());
	}

	@Override
	public NodeContentI suppressNode() throws Exception {
		NodeContent nodeContent = new NodeContent(this.content, this.intervalle,
				(CompositeMapContentManagementEndPoint) this.compositeMapContentManagementEndPointOutbound
						.copyWithSharable());

		this.content.clear();
		this.finalise();
		this.shutdownNow();
		return nodeContent;
	}

	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {

		System.out.println("Reception de la requete 'SPLIT' sur le noeud " + this.intervalle.first());
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.add(computationURI);

			if (loadPolicy.shouldSplitInTwoAdjacentNodes(content.size())) {
				assert this.porttoNewNode != null;
				assert this.porttoNewNode.connected();

				CompositeMapContentManagementEndPoint nouvelEndpointEntreNoeuds = new CompositeMapContentManagementEndPoint();

				String nouveauNoeudURI = this.porttoNewNode.createComponent(DynamicNodeBCM.class.getCanonicalName(),
						new Object[] { this.jvmUri, NOUVEAU_NOEUD_URI,
								(CompositeMapContentManagementEndPoint) nouvelEndpointEntreNoeuds.copyWithSharable(),
								(CompositeMapContentManagementEndPoint) this.compositeMapContentManagementEndPointOutbound
										.copyWithSharable(),
								null });
				this.compositeMapContentManagementEndPointOutbound = (CompositeMapContentManagementEndPoint) nouvelEndpointEntreNoeuds
						.copyWithSharable();

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
				NodeContent contenuSplit = new NodeContent(secondPart, intervalle.split(), null);

				this.porttoNewNode.startComponent(nouveauNoeudURI);
				this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
						.initialiseContent(contenuSplit);

				this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
						.split(computationURI, loadPolicy, caller);

			}
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {

		System.out.println("Reception de la requete 'MERGE' sur le noeud " + this.intervalle.first());
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.add(computationURI);

			NodeState stateSuivant = (NodeState) this.compositeMapContentManagementEndPointOutbound
					.getDHTManagementEndpoint().getClientSideReference().getCurrentState();

			if (loadPolicy.shouldMergeWithNextNode(content.size(), stateSuivant.contentDataSize)) {

				NodeContent contentSuivant = (NodeContent) this.compositeMapContentManagementEndPointOutbound
						.getDHTManagementEndpoint().getClientSideReference().suppressNode();

				this.content.putAll(contentSuivant.content);
				this.intervalle.merge(contentSuivant.intervalle);

				this.compositeMapContentManagementEndPointOutbound = (CompositeMapContentManagementEndPoint) contentSuivant.compositeMapContentManagementEndPointOutbound
						.copyWithSharable();

			}

		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		System.out.println("");
		System.out.println("Création  des cordes sur le noeud " + this.uri);
		System.out.println("");
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

	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		System.out.println("Chord info dans le noeud : " + this.uri);
		if (offset > 0) {
			return this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint()
					.getClientSideReference().getChordInfo(offset - 1);
		}
		return new SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>(
				(CompositeMapContentManagementEndPoint) this.compositeMapContentManagementEndPointInbound
						.copyWithSharable(),
				this.intervalle.first());
	}

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
					System.out.println(
							"Envoi du résultat du 'GET' sur la facade depuis le noeud " +  this.uri);
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
						System.out.println("Gap :" + gap + " min gap :"  + min_gap);
						if (min_gap < gap && min_gap >= 0) {
							gap = min_gap;
							next_endpoint = (ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>) chord.first().copyWithSharable();
						}
					}
					
					if (!next_endpoint.clientSideInitialised()) {
						next_endpoint.initialiseClientSide(this);
					}
										
					next_endpoint.getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller);
					
					next_endpoint.cleanUpClientSide();
				}
			}
		} else {
			// Normalement on ne peut pas revenir à un noeud qui a déjà été visité
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'GET' sur la facade depuis le noeud " +  this.uri);
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}

	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		System.out.println("Reception de la requete 'PUT' le noeud " +  this.uri
				+ " identifiant requete : " + computationURI);

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
					System.out.println(
							"Envoi du résultat du 'PUT' sur la facade depuis le noeud " +  this.uri);
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
		} else {
			// Normalement on ne peut pas revenir à un noeud qui a déjà été visité
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'GET' sur la facade depuis le noeud " + this.uri);
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'REMOVE' le noeud " + this.uri
				+ " identifiant requete : " + computationURI);
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
					System.out.println(
							"Envoi du résultat du 'REMOVE' sur la facade depuis le noeud " + this.uri);
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
		} else {
			// Normalement on ne peut pas revenir à un noeud qui a déjà été visité
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'REMOVE' sur la facade depuis le noeud " + this.uri);
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.remove(computationURI);
		} else {
			this.compositeMapContentManagementEndPointOutbound.getContentAccessEndpoint().getClientSideReference()
					.clearComputation(computationURI);
		}
	}

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

				if (!next_endpoint.clientSideInitialised()) {
					next_endpoint.initialiseClientSide(this);
				}	
				
				next_endpoint.getMapReduceEndpoint().getClientSideReference().parallelMap(computationURI,
						selector, processor,
						// Réduit la profondeur max pour éviter les boucles infinies
						new IgnoreChordsPolicy(policy.getNbreChordsIgnores() + increment));
				
				increment++;
				next_endpoint.cleanUpClientSide();

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

			for (int i = debut; i < chords.size(); i++) {
				
				resultsMapReduce.get(computationURI).add(new CompletableFuture<Serializable>());
				next_endpoint = chords.get(i).first();
				// Envoie la tâche au chord

				if (!next_endpoint.clientSideInitialised()) {
					next_endpoint.initialiseClientSide(this);
				}	
				
				next_endpoint.getMapReduceEndpoint().getClientSideReference().parallelReduce(computationURI,
						reductor, combinator, identityAcc, identityAcc,
						new IgnoreChordsPolicy(policy.getNbreChordsIgnores() +increment),
						this.mapReduceResultReceptionEndPoint);
				increment ++;
				next_endpoint.cleanUpClientSide();

			}

			for (CompletableFuture<Serializable> result : resultsMapReduce.get(computationURI)) {
				@SuppressWarnings("unchecked")
				A resTemporaire = (A) result.get();
				localReduce = combinator.apply(resTemporaire, localReduce);
			}

			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}

			System.out.println("Envoi du résultat du 'MAP REDUCE' sur la facade depuis le noeud " + this.uri);

			caller.getClientSideReference().acceptResult(computationURI, "nom du noeud qui envoie", localReduce);
			caller.cleanUpClientSide();

		}
	}

	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		resultsMapReduce.get(computationURI).get(indice).complete(acc);
		indice++;
	}

	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.startOrigin();
		try {
			if (!this.compositeMapContentManagementEndPointOutbound.clientSideInitialised()) {
				this.compositeMapContentManagementEndPointOutbound.initialiseClientSide(this);
			}
			this.porttoNewNode = new DynamicComponentCreationOutboundPort(this);
//			this.porttoNewNode.publishPort();
//			this.doPortConnection(
//					this.porttoNewNode.getPortURI(),
//					this.jvmUri + AbstractCVM.DCC_INBOUNDPORT_URI_SUFFIX,
//					DynamicComponentCreationConnector.class.getCanonicalName());
			this.chords = new ArrayList<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>>();

			AbstractComponent.checkImplementationInvariant(this);
			AbstractComponent.checkInvariant(this);
		} catch (Exception e) {
			throw new ComponentStartException(e);
		}
	}

	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.printExecutionLogOnFile("node");
		this.compositeMapContentManagementEndPointOutbound.cleanUpClientSide();
		if (this.porttoNewNode.connected()) {
			this.doPortDisconnection(this.porttoNewNode.getPortURI());
		}
		super.finaliseOrigin();
	}

	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.compositeMapContentManagementEndPointInbound.cleanUpServerSide();
			this.porttoNewNode.unpublishPort();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownOrigin();
	}

	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.compositeMapContentManagementEndPointInbound.cleanUpServerSide();
			this.porttoNewNode.unpublishPort();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNowOrigin();
	}

}
