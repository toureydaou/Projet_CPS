package etape4.composants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import etape3.composants.AsynchronousNodeBCM;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape4.endpoints.CompositeMapContentManagementEndPoint;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
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
		ResultReceptionCI.class, MapReduceResultReceptionCI.class })
@OfferedInterfaces(offered = { DHTManagementCI.class, ParallelMapReduceCI.class, ContentAccessCI.class,
 		MapReduceResultReceptionCI.class})
public class NodeBCM extends AsynchronousNodeBCM implements DHTManagementI, ParallelMapReduceI, MapReduceResultReceptionI {

	protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound;
	
	protected CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointInbound;
	
	protected MapReduceResultReceptionEndPoint mapReduceResultReceptionEndPoint;

	protected ArrayList<SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>> chords;
	
	protected DynamicComponentCreationOutboundPort porttoNewNode;
	
	String jvmUri;
	
	private HashMap<String, ArrayList<CompletableFuture<Serializable>>> resultsMapReduce;

	public NodeBCM(String jvmUri, String uri, IntInterval intervalle,
			CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointOutbound,
			CompositeMapContentManagementEndPoint compositeMapContentManagementEndPointInbound) throws ConnectionException {
		super(uri,intervalle);
		this.jvmUri = jvmUri;
		this.compositeMapContentManagementEndPointOutbound = compositeMapContentManagementEndPointOutbound;
		this.compositeMapContentManagementEndPointInbound = compositeMapContentManagementEndPointInbound;
		this.mapReduceResultReceptionEndPoint = new MapReduceResultReceptionEndPoint();
		this.resultsMapReduce = new HashMap<String, ArrayList<CompletableFuture<Serializable>>>();
		
		this.mapReduceResultReceptionEndPoint.initialiseServerSide(this);
		this.compositeMapContentManagementEndPointInbound.initialiseServerSide(this);
	}

	protected static class NodeContent implements NodeContentI {

		private static final long serialVersionUID = 1L;

		public ConcurrentHashMap<Integer, ContentDataI> content;

		public IntInterval intervalle;

		public CopyOnWriteArrayList<String> listeUriContentOperations;

		public CopyOnWriteArrayList<String> listeUriMapOperations;

		public CopyOnWriteArrayList<String> listeUriReduceOperations;

		public ConcurrentHashMap<String, CompletableFuture<Stream<ContentDataI>>> memory;
		

		protected NodeContent(ConcurrentHashMap<Integer, ContentDataI> content, IntInterval intervalle,
				CopyOnWriteArrayList<String> listeUriContentOperations,
				CopyOnWriteArrayList<String> listeUriMapOperations,
				CopyOnWriteArrayList<String> listeUriReduceOperations,
				ConcurrentHashMap<String, CompletableFuture<Stream<ContentDataI>>> memory) {

			this.content = content;
			this.intervalle = intervalle;
			this.listeUriContentOperations = listeUriContentOperations;
			this.listeUriMapOperations = listeUriMapOperations;
			this.listeUriReduceOperations = listeUriReduceOperations;
			this.memory = memory;
		}

	}

	protected static class NodeState implements NodeStateI {

		private static final long serialVersionUID = 1L;

		protected int contentDataSize;

		public NodeState(int contentDataSize) {

			this.contentDataSize = contentDataSize;
		}
	}

	protected NodeBCM(String uri, AsynchronousCompositeMapContentEndPoint compositeMapEndpointInboundAsync,
			AsynchronousCompositeMapContentEndPoint compositeMapEndpointOutboundAsync, IntInterval intervalle)
					throws ConnectionException {
		super(uri, compositeMapEndpointInboundAsync, compositeMapEndpointOutboundAsync, intervalle);

	}

	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		this.content.putAll(((NodeContent) content).content);
		this.memory.putAll(((NodeContent) content).memory);
		this.listeUriContentOperations.addAll(listeUriContentOperations);
		this.listeUriMapOperations.addAll(((NodeContent) content).listeUriMapOperations);
		this.listeUriReduceOperations.addAll(((NodeContent) content).listeUriReduceOperations);
		this.intervalle.merge(((NodeContent) content).intervalle);

	}

	@Override
	public NodeStateI getCurrentState() throws Exception {
		return new NodeState(this.content.size());
	}

	@Override
	public NodeContentI suppressNode() throws Exception {
		NodeContent nodeContent = new NodeContent(this.content, this.intervalle, this.listeUriContentOperations,
				this.listeUriMapOperations, this.listeUriReduceOperations, this.memory);
		return nodeContent;
	}

	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {

	}

//	@Override
//	public <CI extends ResultReceptionCI> void split(
//	        String computationURI,
//	        LoadPolicyI loadPolicy,
//	        EndPointI<CI> caller
//	) throws Exception {
//	    // 1. Vérifier que la charge justifie un split
//	    int currentSize = this.content.size();
//	    final int THRESHOLD = 30;  // ton seuil
//	    if (currentSize <= THRESHOLD) {
//	        // rien à faire, on renvoie juste un OK (ou null)
//	        caller.getClientSideReference()
//	              .acceptResult(computationURI, null);
//	        return;
//	    }
//
//	    // 2. Calculer la nouvelle intervalle
//	    int start = this.intervalle.first();
//	    int end   = this.intervalle.last();
//	    int mid   = (start + end) / 2;
//	    IntInterval oldInterval = this.intervalle;
//	    IntInterval leftInterval  = new IntInterval(start, mid);
//	    IntInterval rightInterval = new IntInterval(mid + 1, end);
//
//	    // 3. Mettre à jour l'intervalle de ce noeud (moitié gauche)
//	    this.intervalle = leftInterval;
//
//	    // 4. Créer dynamiquement le nouveau noeud pour la moitié droite
//	    // ----------------------------------------------------------------
//	    // TODO #1 : tu dois disposer d’un DynamicComponentCreationOutboundPort (dccOutPort)
//	    //           initialisé et connecté dans start(), et d’une liste deployedComponentsURIs.
//	    //           Il faut donc :
//	    //             - ajouter un champ `protected DynamicComponentCreationOutboundPort dccOutPort;`
//	    //             - dans start(), faire `dccOutPort = new ...; publish(); doPortConnection(...);`
//	    //
//	    // Exemple de création :
//	    String newNodeURI = this.dccOutPort.createComponent(
//	        NodeBCM.class.getCanonicalName(),
//	        new Object[]{
//	            /* URI du port réflexif du nouveau noeud, 
//	               AsynchronousCompositeMapContentEndPoint inbound, 
//	               AsynchronousCompositeMapContentEndPoint outbound, 
//	               rightInterval */
//	        }
//	    );
//	    this.deployedComponentsURIs.add(newNodeURI);
//	    this.dccOutPort.startComponent(newNodeURI);
//
//	    // 5. Récupérer le port DHTManagement du nouveau noeud via ReflectionCI
//	    // --------------------------------------------------------------------
//	    // TODO #2 : instancier et connecter un ReflectionOutboundPort ici,
//	    //           pour faire rop.findInboundPortURIsFromInterface(DHTManagementCI.class)
//	    String reflectionPortURI = /* URI du port réflexif du nouveau noeud */;
//	    ReflectionOutboundPort rop = new ReflectionOutboundPort(this);
//	    rop.publishPort();
//	    this.doPortConnection(
//	        rop.getPortURI(),
//	        reflectionPortURI,
//	        ReflectionConnector.class.getCanonicalName()
//	    );
//	    String[] inbound = rop.findInboundPortURIsFromInterface(DHTManagementCI.class);
//	    String newNodeDHTInURI = inbound[0];
//	    rop.doPortDisconnection(rop.getPortURI());
//	    rop.unpublishPort();
//
//	    // 6. Transférer les données de l’intervalle droite
//	    // ------------------------------------------------
//	    // on crée un NodeContent encapsulant le sous-ensemble à migrer
//	    NodeContentI toTransfer = this.suppressNode();     // récupère tout
//	    // TODO #3 : filtrer `toTransfer.content` pour ne garder que les clés ∈ rightInterval
//	    //           et mettre à jour `this.content` pour supprimer ces clés locales
//	    // ensuite, on appelle initialiseContent du nouveau noeud
//	    DHTManagementCI newNode = this.createManagementStub(newNodeDHTInURI);
//	    newNode.initialiseContent(toTransfer);
//
//	    // 7. Mettre à jour les chords sur les deux noeuds
//	    // ------------------------------------------------
//	    // TODO #4 : appeler computeChords(...) sur ce noeud et sur newNode
//	    this.computeChords(computationURI, this.chords.size());
//	    newNode.computeChords(computationURI, this.chords.size());
//
//	    // 8. Retourner un résultat au caller (optionnel)
//	    caller.getClientSideReference()
//	          .acceptResult(computationURI, null);
//	
//	}
//	
	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {

	}

	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		this.chords.clear();
		int offset = 1;
		for (int i = 0; i < numberOfChords +1; i++) {
			if (this.getChordInfo(offset).second() >= this.intervalle.first()) {
				this.chords.add(this.getChordInfo(offset));
			}
			offset = offset * 2;
		}
	}

	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		if (offset > 0) {
			return this.compositeMapContentManagementEndPointOutbound.getDHTManagementEndpoint().getClientSideReference()
					.getChordInfo(offset - 1);
		}
		return new SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>(
				this.compositeMapContentManagementEndPointOutbound, this.intervalle.first());
	}

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'GET' sur le noeud " + this.intervalle.first());
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
				next_endpoint.getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller);
			}

		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'GET' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}

	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		System.out.println("Reception de la requete 'PUT' le noeud " + this.intervalle.first()
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
				next_endpoint.getContentAccessEndpoint().getClientSideReference().put(computationURI, key, value,
						caller);
			}
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'PUT' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'REMOVE' le noeud " + this.intervalle.first()
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
				next_endpoint.getContentAccessEndpoint().getClientSideReference().remove(computationURI, key,
						caller);
			}
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out
			.println("Envoi du résultat du 'REMOVE' sur la facade depuis le noeud " + this.intervalle.first());
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		super.clearComputation(computationURI);
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		super.clearMapReduceComputation(computationURI);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {

		System.out.println("Reception de la requete 'MAP REDUCE' (MAP) sur le noeud " + this.intervalle.first()
		+ " identifiant requete : " + computationURI);
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.addIfAbsent(computationURI);

			// Vérification du type de politique
			if (!(parallelismPolicy instanceof IgnoreChordsPolicy)) {
				throw new IllegalArgumentException("Unsupported parallelism policy");
			}
			IgnoreChordsPolicy policy = (IgnoreChordsPolicy)parallelismPolicy;

			int debut = Math.min(chords.size(), policy.getNbreChordsIgnores());

			for(int i=debut; i<chords.size(); i++) {
				// Envoie la tâche au chord
				chords.get(i).first().getMapReduceEndpoint()
				.getClientSideReference()
				.parallelMap(
						computationURI, 
						selector, 
						processor, 
						// Réduit la profondeur max pour éviter les boucles infinies
						new IgnoreChordsPolicy(policy.getNbreChordsIgnores()+i)
						);

			}

			//Calcul dans le noeud
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

		System.out.println("Reception de la requete 'MAP REDUCE' (REDUCE) sur le noeud " + this.intervalle.first()
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
			IgnoreChordsPolicy policy = (IgnoreChordsPolicy)parallelismPolicy;

			int debut = Math.min(chords.size(), policy.getNbreChordsIgnores());
			
			A resTemporaire = localReduce;
			for(int i=debut; i<chords.size(); i++) {
				// Envoie la tâche au chord
				chords.get(i).first().getMapReduceEndpoint()
						.getClientSideReference()
						.parallelReduce(computationURI,
								reductor,
								combinator,
								identityAcc,
								resTemporaire, 
								parallelismPolicy,
								this.mapReduceResultReceptionEndPoint);
				resTemporaire = identityAcc;
			}

			
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println(
					"Envoi du résultat du 'MAP REDUCE' sur la facade depuis le noeud " + this.intervalle.first());
			//
			caller.getClientSideReference().acceptResult(computationURI, "nom du noeud qui envoie", currentAcc);
			caller.cleanUpClientSide();
		}
	}
	
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		
		
	}
	
	
	public void start2() throws ComponentStartException {
	    super.start();
	    try {
	        // 1. Création et publication du port DCC
	        this.dccOutPort = new DynamicComponentCreationOutboundPort(this);
	        this.dccOutPort.publishPort();

	        // 2. Connexion de ce port à la DCC inbound port de la JVM
	        //    Remplace `this.jvmURI` par la façon dont tu stockes l'URI de ta JVM
	        this.doPortConnection(
	            this.dccOutPort.getPortURI(),
	            this.jvmURI + AbstractCVM.DCC_INBOUNDPORT_URI_SUFFIX,
	            DynamicComponentCreationConnector.class.getCanonicalName()
	        );

	        // 3. Initialisation de la table de chords vide
	        this.chords = new ArrayList<>();

	    } catch (Exception e) {
	        throw new ComponentStartException(e);
	    }
	}
	
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.startOrigin();
		try {
			if (!this.compositeMapContentManagementEndPointOutbound.clientSideInitialised()) {
				this.compositeMapContentManagementEndPointOutbound.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	
	}



}
