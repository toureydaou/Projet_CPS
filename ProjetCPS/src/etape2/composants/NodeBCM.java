package etape2.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import etape2.endpoints.CompositeMapContentSyncEndpoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

//-----------------------------------------------------------------------------
/**
 * La classe <code>NodeBCM</code> représente un nœud d'une table de hachage
 * répartie (DHT). Elle offre des services de stockage et de récupération de
 * données, ainsi que des fonctionnalités de calcul distribué basées sur
 * MapReduce.
 * 
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Chaque instance de <code>NodeBCM</code> gère un intervalle spécifique de clés
 * et communique avec d'autres noeuds via des ports entrants et sortants.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class NodeBCM extends AbstractComponent implements MapReduceSyncI, ContentAccessSyncI{

	// Stocke les données associées aux clés de la DHT
	protected HashMap<ContentKeyI, ContentDataI> content;

	// Définition de l'intervalle de clés géré par ce nœud
	protected IntInterval intervalle;

	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> uriPassCont = new CopyOnWriteArrayList<>();
	protected CopyOnWriteArrayList<String> uriPassMap = new CopyOnWriteArrayList<>();
	

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	// MapReduce
	private HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();

	// Ports pour la communication avec d'autres noeuds
	protected CompositeMapContentSyncEndpoint cmceInbound; // Port entrant (serveur)
	protected CompositeMapContentSyncEndpoint cmceOutbound; // Port sortant (client)

	private static final int SCHEDULABLE_THREADS = 2;
	private static final int THREADS_NUMBER = 0;

	/**
	 * Constructeur d'un nœud de la DHT.
	 * 
	 * @param uri          URI du composant
	 * @param cmceInbound  Port entrant du composant
	 * @param cmceOutbound Port sortant du composant
	 * @param intervalle   Intervalle de clés gérées par ce noeud
	 * @throws ConnectionException en cas d'erreur de connexion
	 */
	protected NodeBCM(String uri, CompositeMapContentSyncEndpoint cmceInbound, CompositeMapContentSyncEndpoint cmceOutbound,
			IntInterval intervalle) throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.content = new HashMap<>();
		this.intervalle = intervalle;
		this.cmceInbound = cmceInbound;
		this.cmceOutbound = cmceOutbound;
		cmceInbound.initialiseServerSide(this);
	}
	
	protected NodeBCM(int nbreThreads, int nbreThreadsSchedulables) throws ConnectionException {
		super(nbreThreads, nbreThreadsSchedulables);
	}
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#mapSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		System.out.println("Reception de la requête 'MAP REDUCE' (MAP) sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (!uriPassMap.contains(computationURI)) {
			uriPassMap.add(computationURI);
			memory.put(computationURI,
					(Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));
			this.cmceOutbound.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector,
					processor);
		} else {
			return;
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#reduceSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		System.out.println("Reception de la requête 'MAP REDUCE' (REDUCE) sur le noeud " + this.intervalle.first()
				+ " - " + this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassMap.contains(computationURI)) {
			uriPassMap.remove(computationURI);
			return combinator.apply(
					memory.get(computationURI).reduce(currentAcc, (u, d) -> reductor.apply(u, (R) d), combinator),
					this.cmceOutbound.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI,
							reductor, combinator, currentAcc));
		} else {
			return currentAcc;
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#clearMapReduceComputation(java.lang.String)
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		System.out.println("Nettoyage des opérations du map reduce sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (memory.containsKey(computationURI)) {
			memory.remove(computationURI);
			this.cmceOutbound.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
		}

	}
	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#getSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		System.out.println("Reception de la requête 'GET' sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int cle = key.hashCode();
			if (intervalle.in(cle)) {
				return content.get(key);
			}
			return this.cmceOutbound.getContentAccessEndpoint().getClientSideReference().getSync(computationURI, key);
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#putSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		System.out.println("Reception de la requête 'PUT' sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int cle = key.hashCode();
			if (intervalle.in(cle)) {
				ContentDataI valuePrec = content.get(key);
				content.put(key, value);
				return valuePrec;
			}
			return this.cmceOutbound.getContentAccessEndpoint().getClientSideReference().putSync(computationURI, key,
					value);

		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#removeSync(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		System.out.println("Reception de la requête 'REMOVE' sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int n = key.hashCode();
			if (intervalle.in(n)) {
				ContentDataI valuePrec = content.remove(key);
				return valuePrec;
			}
			return this.cmceOutbound.getContentAccessEndpoint().getClientSideReference().removeSync(computationURI,
					key);
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#clearComputation(java.lang.String)
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		System.out.println("Nettoyage sur le noeud " + this.intervalle.first() + " - " + this.intervalle.last()
				+ ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			uriPassCont.remove(computationURI);
			this.cmceOutbound.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		}
	}


	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#start()
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.start();
		try {
			if (!this.cmceOutbound.clientSideInitialised()) {
				this.cmceOutbound.initialiseClientSide(this);
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
		this.cmceOutbound.cleanUpClientSide();
		super.finalise();
	}

	
	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.cmceInbound.cleanUpServerSide();
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
			this.cmceInbound.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}
	
	/**
	 * Start origin.
	 *
	 * @throws ComponentStartException the component start exception
	 */
	public void startOrigin() throws ComponentStartException {
		super.start();
	}
	
	/**
	 * Finalise origin.
	 *
	 * @throws Exception the exception
	 */
	public void finaliseOrigin() throws Exception {
		super.finalise();
	}
	
	/**
	 * Shutdown origin.
	 *
	 * @throws ComponentShutdownException the component shutdown exception
	 */
	public void shutdownOrigin() throws ComponentShutdownException {
		super.shutdown();
	}
	
	/**
	 * Shutdown now origin.
	 *
	 * @throws ComponentShutdownException the component shutdown exception
	 */
	public void shutdownNowOrigin() throws ComponentShutdownException {
		super.shutdownNow();
	}

}
