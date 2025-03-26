package etape3.composants;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

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
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
		MapReduceResultReceptionCI.class })
public class AsynchronousNodeBCM extends AbstractComponent implements ContentAccessI, MapReduceI {

	// Stocke les données associées aux clés de la DHT
	protected ConcurrentHashMap<ContentKeyI, ContentDataI> content;

	protected IntInterval intervalle;

	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> listeUriContentOperations = new CopyOnWriteArrayList<>();
	protected CopyOnWriteArrayList<String> listeUriMapOperations = new CopyOnWriteArrayList<>();
	protected CopyOnWriteArrayList<String> listeUriReduceOperations = new CopyOnWriteArrayList<>();

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	// MapReduce
	private ConcurrentHashMap<String, CompletableFuture<Stream<ContentDataI>>> memory = new ConcurrentHashMap<>();

	protected AsynchronousCompositeMapContentEndPoint compositeMapContentEndpointOutboundAsync;
	protected AsynchronousCompositeMapContentEndPoint compositeMapContentEndpointInboundAsync;

	protected AsynchronousNodeBCM(String uri, AsynchronousCompositeMapContentEndPoint compositeMapEndpointInboundAsync,
			AsynchronousCompositeMapContentEndPoint compositeMapEndpointOutboundAsync, IntInterval intervalle)
			throws ConnectionException {
		super(1, 1);
		this.content = new ConcurrentHashMap<>();
		this.intervalle = intervalle;
		this.compositeMapContentEndpointInboundAsync = compositeMapEndpointInboundAsync;
		this.compositeMapContentEndpointOutboundAsync = compositeMapEndpointOutboundAsync;
		this.compositeMapContentEndpointInboundAsync.initialiseServerSide(this);
	}

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'GET' sur le noeud " + this.intervalle.first());
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				if (!caller.clientSideInitialised()) {
					caller.initialiseClientSide(this);
				}
				caller.getClientSideReference().acceptResult(computationURI, content.get(key));
				caller.cleanUpClientSide();
			} else {
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
						.get(computationURI, key, caller);
			}

		} else {
			System.out.println(
					"Envoi du résultat du 'GET' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
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
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
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
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
						.remove(computationURI, key, caller);

			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
		System.out.println("Reception de la requete 'MAP REDUCE' (MAP) sur le noeud " + this.intervalle.first());
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.addIfAbsent(computationURI);

			CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
			memory.putIfAbsent(computationURI, futureStream);
			futureStream.complete((Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));

			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndPoint().getClientSideReference().map(computationURI,
					selector, processor);

		}
		

	}

	@SuppressWarnings("unchecked")
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		System.out.println("Reception de la requete 'MAP REDUCE' (REDUCE) sur le noeud " + this.intervalle.first());
		if (!listeUriReduceOperations.contains(computationURI)) {
			listeUriReduceOperations.add(computationURI);

			Stream<ContentDataI> localStream = memory.get(computationURI).get();

			A localReduce = localStream.reduce(identityAcc, (u, d) -> reductor.apply(u, (R) d), combinator);
			localReduce = combinator.apply(currentAcc, localReduce);
			
			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndPoint().getClientSideReference()
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
	
	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.remove(computationURI);
		} else {
			this.compositeMapContentEndpointOutboundAsync.getContentAccessEndPoint().getClientSideReference()
			.clearComputation(computationURI);
		}
	}
	
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if ( listeUriMapOperations.contains(computationURI) && listeUriReduceOperations.contains(computationURI)) {
			this.listeUriMapOperations.remove(computationURI);
			this.listeUriReduceOperations.remove(computationURI);
			this.memory.remove(computationURI);
		} else {
			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndPoint().getClientSideReference()
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

	
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.printExecutionLogOnFile("node");
		this.compositeMapContentEndpointOutboundAsync.cleanUpClientSide();
		super.finalise();
	}

	
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.compositeMapContentEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.compositeMapContentEndpointInboundAsync.cleanUpServerSide();
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

	

}
