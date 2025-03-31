package etape3.composants;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import etape3.utils.ThreadsPolicy;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

// TODO: Auto-generated Javadoc
/**
 * The Class AsynchronousNodeBCM.
 */
@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
		MapReduceResultReceptionCI.class })
public class AsynchronousNodeBCM extends AbstractComponent implements ContentAccessI, MapReduceI {
	
	private static final int SCHEDULABLE_THREADS = 2;
	private static final int THREADS_NUMBER = 0;

	/** The Constant CONTENT_ACCESS_HANDLER_URI. */
	private static final String CONTENT_ACCESS_HANDLER_URI = "Content-Access-Pool-Threads";
	
	/** The Constant MAP_REDUCE_HANDLER_URI. */
	private static final String MAP_REDUCE_HANDLER_URI = "Content-Access-Pool-Threads";

	/** The content. */
	// Stocke les données associées aux clés de la DHT
	protected ConcurrentHashMap<Integer, ContentDataI> content;

	/** The intervalle. */
	protected IntInterval intervalle;

	/** The liste uri content operations. */
	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> listeUriContentOperations = new CopyOnWriteArrayList<>();
	
	/** The liste uri map operations. */
	protected CopyOnWriteArrayList<String> listeUriMapOperations = new CopyOnWriteArrayList<>();
	
	/** The liste uri reduce operations. */
	protected CopyOnWriteArrayList<String> listeUriReduceOperations = new CopyOnWriteArrayList<>();

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	/** The memory. */
	// MapReduce
	private ConcurrentHashMap<String, CompletableFuture<Stream<ContentDataI>>> memory = new ConcurrentHashMap<>();

	/** The composite map content endpoint outbound async. */
	protected AsynchronousCompositeMapContentEndPoint compositeMapContentEndpointOutboundAsync;
	
	/** The composite map content endpoint inbound async. */
	protected AsynchronousCompositeMapContentEndPoint compositeMapContentEndpointInboundAsync;

	/** The hash map lock. */
	protected final ReentrantReadWriteLock hashMapLock;

	/**
	 * Instantiates a new asynchronous node BCM.
	 *
	 * @param uri the uri
	 * @param compositeMapEndpointInboundAsync the composite map endpoint inbound async
	 * @param compositeMapEndpointOutboundAsync the composite map endpoint outbound async
	 * @param intervalle the intervalle
	 * @throws ConnectionException the connection exception
	 */
	protected AsynchronousNodeBCM(String uri, AsynchronousCompositeMapContentEndPoint compositeMapEndpointInboundAsync,
			AsynchronousCompositeMapContentEndPoint compositeMapEndpointOutboundAsync, IntInterval intervalle)
			throws ConnectionException {
		super(THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.content = new ConcurrentHashMap<>();
		this.intervalle = intervalle;
		this.compositeMapContentEndpointInboundAsync = compositeMapEndpointInboundAsync;
		this.compositeMapContentEndpointOutboundAsync = compositeMapEndpointOutboundAsync;
		this.hashMapLock = new ReentrantReadWriteLock();

		this.compositeMapContentEndpointInboundAsync.setExecutorServiceIndexContentAccessService(
				this.createNewExecutorService(URIGenerator.generateURI(CONTENT_ACCESS_HANDLER_URI),
						ThreadsPolicy.NUMBER_CONTENT_ACCESS_THREADS, true));

		this.compositeMapContentEndpointInboundAsync.setExecutorServiceIndexContentAccessService(
				this.createNewExecutorService(URIGenerator.generateURI(MAP_REDUCE_HANDLER_URI),
						ThreadsPolicy.NUMBER_MAP_REDUCE_THREADS, true));

		this.compositeMapContentEndpointInboundAsync.initialiseServerSide(this);

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI#get(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
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
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
						.get(computationURI, key, caller);
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

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI#put(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		System.out.println("Reception de la requete 'PUT' le noeud " + this.intervalle.first() + " identifiant requete : " + computationURI);
		
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
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
						.put(computationURI, key, value, caller);
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

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI#remove(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'REMOVE' le noeud " + this.intervalle.first() + " identifiant requete : " + computationURI);
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
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
						.remove(computationURI, key, caller);
			}
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'REMOVE' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI#map(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
		System.out.println("Reception de la requete 'MAP REDUCE' (MAP) sur le noeud " + this.intervalle.first() + " identifiant requete : " + computationURI);
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.addIfAbsent(computationURI);
			this.hashMapLock.readLock().lock();
			try {
				

				CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
				memory.putIfAbsent(computationURI, futureStream);
				memory.get(computationURI).complete((Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));

				this.compositeMapContentEndpointOutboundAsync.getMapReduceEndpoint().getClientSideReference()
						.map(computationURI, selector, processor);
			} finally {
				this.hashMapLock.readLock().unlock();
			}	
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI#reduce(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A, A, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		System.out.println("Reception de la requete 'MAP REDUCE' (REDUCE) sur le noeud " + this.intervalle.first() + " identifiant requete : " + computationURI);
		if (!listeUriReduceOperations.contains(computationURI)) {
			listeUriReduceOperations.add(computationURI);
			
			CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
			memory.putIfAbsent(computationURI, futureStream);
			
			Stream<ContentDataI> localStream = memory.get(computationURI).get();

			A localReduce = localStream.reduce(identityAcc, (u, d) -> reductor.apply(u, (R) d), combinator);
			localReduce = combinator.apply(currentAcc, localReduce);

			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndpoint().getClientSideReference()
					.reduce(computationURI, reductor, combinator, identityAcc, localReduce, callerNode);
		} else {
			if (!callerNode.clientSideInitialised()) {
				callerNode.initialiseClientSide(this);
			}
			System.out.println(
					"Envoi du résultat du 'MAP REDUCE' sur la facade depuis le noeud " + this.intervalle.first());
			callerNode.getClientSideReference().acceptResult(computationURI, "", currentAcc);
			callerNode.cleanUpClientSide();
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#clearComputation(java.lang.String)
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.remove(computationURI);
		} else {
			this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
					.clearComputation(computationURI);
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#clearMapReduceComputation(java.lang.String)
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (listeUriMapOperations.contains(computationURI) && listeUriReduceOperations.contains(computationURI)) {
			this.listeUriMapOperations.remove(computationURI);
			this.listeUriReduceOperations.remove(computationURI);
			this.memory.remove(computationURI);
		} else {
			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndpoint().getClientSideReference()
					.clearMapReduceComputation(computationURI);
		}
	}

	/**
	 * Démarre le composant ClientBCM.
	 * 
	 * @throws ComponentStartException Si une erreur se produit lors du démarrage du
	 *                                 composant.
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.start();
		try {
			if (!this.compositeMapContentEndpointOutboundAsync.clientSideInitialised()) {
				this.compositeMapContentEndpointOutboundAsync.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#finalise()
	 */
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.printExecutionLogOnFile("node");
		this.compositeMapContentEndpointOutboundAsync.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.compositeMapContentEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdownNow()
	 */
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.compositeMapContentEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#getSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#putSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#removeSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#mapSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI)
	 */
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#reduceSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}