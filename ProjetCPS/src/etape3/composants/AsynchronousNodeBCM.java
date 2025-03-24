package etape3.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import etape1.EntierKey;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
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

@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class, MapReduceResultReceptionCI.class })
public class AsynchronousNodeBCM extends AbstractComponent implements ContentAccessI, MapReduceI {

	// Stocke les données associées aux clés de la DHT
	protected ConcurrentHashMap<ContentKeyI, ContentDataI> content;
	
	

	protected IntInterval intervalle;

	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> listeUriContentOperations = new CopyOnWriteArrayList<>();
	protected CopyOnWriteArrayList<String> listeUriMapOperations = new CopyOnWriteArrayList<>();

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	// MapReduce
	private ConcurrentHashMap<String, CompletableFuture<Stream<ContentDataI>>> memory = new ConcurrentHashMap<>();

	protected AsynchronousCompositeMapContentEndPoint compositeMapEndpointOutboundAsync;
	protected AsynchronousCompositeMapContentEndPoint compositeMapEndpointInboundAsync;

	protected AsynchronousNodeBCM(String uri, AsynchronousCompositeMapContentEndPoint compositeMapEndpointInboundAsync,
			AsynchronousCompositeMapContentEndPoint compositeMapEndpointOutboundAsync, IntInterval intervalle)
			throws ConnectionException {
		super(2, 0);
		this.content = new ConcurrentHashMap<>();
		this.intervalle = intervalle;
		this.compositeMapEndpointInboundAsync = compositeMapEndpointInboundAsync;
		this.compositeMapEndpointOutboundAsync = compositeMapEndpointOutboundAsync;
		this.compositeMapEndpointInboundAsync.initialiseServerSide(this);
	}

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {

		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				if (!caller.clientSideInitialised()) {
					caller.initialiseClientSide(this);
				}
				caller.getClientSideReference().acceptResult(computationURI, content.get(key));
				caller.cleanUpClientSide();
			} else {
				this.compositeMapEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
						.get(computationURI, key, caller);
			}

		}

	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {

		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				if (!caller.clientSideInitialised()) {
					caller.initialiseClientSide(this);
				}
				ContentDataI oldValue = content.putIfAbsent(key, value);
				caller.getClientSideReference().acceptResult(computationURI, oldValue);
				caller.cleanUpClientSide();
			} else {
				this.compositeMapEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
						.put(computationURI, key, value, caller);
			}
		}
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				if (!caller.clientSideInitialised()) {
					caller.initialiseClientSide(this);
				}
				ContentDataI oldValue = content.remove(key);
				caller.getClientSideReference().acceptResult(computationURI, oldValue);
				caller.cleanUpClientSide();
			} else {
				this.compositeMapEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
						.remove(computationURI, key, caller);

			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.addIfAbsent(computationURI);
			
			CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
			memory.putIfAbsent(computationURI, futureStream);
			futureStream.complete((Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));
			

			this.compositeMapEndpointOutboundAsync.getMapReduceEndPoint().getClientSideReference().map(computationURI,
					selector, processor);

		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.remove(computationURI);
			
			Stream<ContentDataI> localStream = memory.get(computationURI).get();
			
			A localReduce = localStream.reduce(identityAcc, (u, d) -> reductor.apply(u, (R) d), combinator);
			localReduce = combinator.apply(currentAcc, localReduce);
			this.compositeMapEndpointOutboundAsync.getMapReduceEndPoint().getClientSideReference().reduce(computationURI, reductor, combinator, identityAcc, localReduce, callerNode);	

		} else {
			if (!callerNode.clientSideInitialised()) {
				callerNode.initialiseClientSide(this);
			}
			callerNode.getClientSideReference().acceptResult(computationURI, "" , currentAcc);
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
			if (!this.compositeMapEndpointOutboundAsync.clientSideInitialised()) {
				this.compositeMapEndpointOutboundAsync.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * Finalise et arrête le composant ClientBCM.
	 * 
	 * @throws Exception Si une erreur se produit lors de l'arrêt du composant.
	 */
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.printExecutionLogOnFile("node");
		this.compositeMapEndpointOutboundAsync.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * Effectue un arrêt propre du composant ClientBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt.
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.compositeMapEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * Force un arrêt immédiat du composant ClientBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt
	 *                                    immédiat.
	 */
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.compositeMapEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub

	}

}
