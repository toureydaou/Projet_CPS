package etape3.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorCompletionService;
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
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class })
public class AsynchronousNodeBCM extends AbstractComponent implements ContentAccessI, MapReduceI {

	// Stocke les données associées aux clés de la DHT
	protected ConcurrentHashMap<ContentKeyI, ContentDataI> content;

	protected IntInterval intervalle;

	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> uriPassCont = new CopyOnWriteArrayList<>();
	protected CopyOnWriteArrayList<String> uriPassMap = new CopyOnWriteArrayList<>();

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	// MapReduce
	private HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();

	protected AsynchronousCompositeMapContentEndPoint cmceOutboundAsync;
	protected AsynchronousCompositeMapContentEndPoint cmceInboundAsync;

	protected AsynchronousNodeBCM(String uri, AsynchronousCompositeMapContentEndPoint cmceInboundAsync,
			AsynchronousCompositeMapContentEndPoint cmceOutboundAsync, IntInterval intervalle)
			throws ConnectionException {
		super(2, 0);
		this.content = new ConcurrentHashMap<>();
		this.intervalle = intervalle;
		this.cmceInboundAsync = cmceInboundAsync;
		this.cmceOutboundAsync = cmceOutboundAsync;
		this.cmceInboundAsync.initialiseServerSide(this);
	}

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {

		if (!uriPassCont.contains(computationURI)) {
			uriPassCont.addIfAbsent(computationURI);
			ExecutorCompletionService<ContentDataI> ecs = new ExecutorCompletionService<>(this.getExecutorService());

			ecs.submit(() -> {
				if (this.intervalle.in(((EntierKey) key).getCle())) {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					caller.getClientSideReference().acceptResult(computationURI, this.get(key));
					caller.cleanUpClientSide();
				}
				return null;
			});

			ecs.submit(() -> {
				this.cmceOutboundAsync.getContentAccessEndPoint().getClientSideReference().get(computationURI, key,
						caller);
				return null;
			});
		}

	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {

		if (!uriPassCont.contains(computationURI)) {
			uriPassCont.addIfAbsent(computationURI);
			ExecutorCompletionService<ContentDataI> ecs = new ExecutorCompletionService<>(this.getExecutorService());

			ecs.submit(() -> {
				if (this.intervalle.in(((EntierKey) key).getCle())) {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					ContentDataI oldValue = this.get(key);
					this.put(key, value);
					caller.getClientSideReference().acceptResult(computationURI, oldValue);
					caller.cleanUpClientSide();
				}
				return null;
			});

			ecs.submit(() -> {
				this.cmceOutboundAsync.getContentAccessEndPoint().getClientSideReference().put(computationURI, key,
						value, caller);
				return null;
			});
		}
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		if (!uriPassCont.contains(computationURI)) {
			uriPassCont.addIfAbsent(computationURI);
			ExecutorCompletionService<ContentDataI> ecs = new ExecutorCompletionService<>(this.getExecutorService());

			ecs.submit(() -> {
				if (this.intervalle.in(((EntierKey) key).getCle())) {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					ContentDataI oldValue = this.get(key);
					this.remove(key);
					caller.getClientSideReference().acceptResult(computationURI, oldValue);
					caller.cleanUpClientSide();
				}
				return null;
			});

			ecs.submit(() -> {
				this.cmceOutboundAsync.getContentAccessEndPoint().getClientSideReference().remove(computationURI, key,
						caller);
				return null;
			});
		}
	}

	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * Récupère les données associées à la clé spécifiée {@code key} dans la
	 * collection locale de données {@code content}. Étant donnée qu'il s'agit des
	 * réferences qui sont utilisées pour stocker nos données dans la DHT, on
	 * récupère donc dans la DHT la clé ayant la même valeur que {@code key} puis on
	 * récupère la valeur associé à la clé correspondante.
	 * 
	 * @param key La clé des données à récupérer.
	 * @return Les données associées à la clé {@code key} si elles existent, sinon
	 *         {@code null}.
	 */
	protected ContentDataI get(ContentKeyI key) {
		for (ContentKeyI k : content.keySet()) {

			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {

				return content.get(k);
			}
		}
		return null;
	}

	/**
	 * Insère ou met à jour les données associées à la clé spécifiée {@code key}
	 * dans la collection locale de données {@code content}.
	 * 
	 * Si la clé existe déjà dans la collection, elle sera mise à jour avec la
	 * nouvelle valeur {@code data}. Sinon, la clé et la valeur seront ajoutées à la
	 * collection.
	 * 
	 * @param key  La clé sous laquelle les données doivent être insérées ou mises à
	 *             jour.
	 * @param data Les données à associer à la clé spécifiée.
	 */
	protected void put(ContentKeyI key, ContentDataI data) {
		for (ContentKeyI k : content.keySet()) {
			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {
				content.putIfAbsent(key, data);
				break;
			}
		}
		content.put(key, data);
	}

	/**
	 * Supprime les données associées à la clé spécifiée {@code key} dans la
	 * collection locale de données {@code content}.
	 * 
	 * La méthode parcourt les clés dans la collection {@code content} et supprime
	 * la donnée associée à la clé spécifiée si une correspondance est trouvée.
	 * 
	 * @param key La clé des données à supprimer.
	 */
	protected void remove(ContentKeyI key) {
		for (ContentKeyI k : content.keySet()) {
			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {
				content.remove(key);
				break;
			}
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
			if (!this.cmceOutboundAsync.clientSideInitialised()) {
				this.cmceOutboundAsync.initialiseClientSide(this);
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
		this.cmceOutboundAsync.cleanUpClientSide();
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
			this.cmceInboundAsync.cleanUpServerSide();
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
			this.cmceInboundAsync.cleanUpServerSide();
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
