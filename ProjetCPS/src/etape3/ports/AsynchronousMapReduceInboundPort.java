package etape3.ports;

import java.io.Serializable;

import etape2.ports.MapReduceSyncInboundPort;
import etape4.policies.ThreadsPolicy;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>AsynchronousMapReduceInboundPort</code> implémente un port
 * entrant pour la réception d'opérations MapReduce dans un système DHT de type
 * MapReduce.
 * 
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * Ce port est utilisé par un composant serveur (Noeud) pour recevoir des
 * requêtes de type Map, Reduce ou Clear de manière asynchrone.
 * 
 * Les appels entrants sont exécutés dans un service d'exécution spécifique
 * identifié par {@code executorServiceIndex}.
 * 
 * Les méthodes asynchrones (`map`, `reduce`, `clearMapReduceComputation`) sont
 * pleinement supportées. Les méthodes synchrones (`mapSync`, `reduceSync`) ne
 * sont pas prises en charge.
 * 
 * <p>
 * <strong>Usage</strong>
 * </p>
 * 
 * Ce port doit être attaché à un composant propriétaire implémentant
 * l'interface {@code MapReduceI}. Il assure l'exécution concurrente des
 * requêtes entrantes dans un contexte multi-threadé.
 * 
 * <p>
 * <strong>Invariant</strong>
 * </p>
 * 
 * <ul>
 * <li>Le composant propriétaire doit implémenter {@code MapReduceI}.</li>
 * <li>Le composant propriétaire doit avoir un service d'exécution valide pour
 * {@code executorServiceIndex}.</li>
 * </ul>
 *
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 * 
 */
//-----------------------------------------------------------------------------

public class AsynchronousMapReduceInboundPort extends MapReduceSyncInboundPort implements MapReduceCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The executor service index. */
	protected final int executorServiceIndex;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Construit un port entrant MapReduce avec une URI spécifiée, l'interface
	 * offerte et un propriétaire donné.
	 *
	 * @param uri                  URI du port.
	 * @param implementedInterface Interface offerte par le port.
	 * @param owner                Composant propriétaire.
	 * @param executorServiceIndex Index du service d'exécution à utiliser.
	 * @throws Exception En cas d'erreur lors de l'initialisation.
	 */
	public AsynchronousMapReduceInboundPort(String uri, Class<? extends OfferedCI> implementedInterface,
			ComponentI owner, int executorServiceIndex) throws Exception {
		super(uri, (Class<? extends OfferedCI>) implementedInterface, owner);
		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * Construit un port entrant MapReduce sans URI explicite.
	 *
	 * @param implementedInterface Interface offerte par le port.
	 * @param owner                Composant propriétaire.
	 * @param executorServiceIndex Index du service d'exécution à utiliser.
	 * @throws Exception En cas d'erreur lors de l'initialisation.
	 */
	public AsynchronousMapReduceInboundPort(Class<? extends OfferedCI> implementedInterface, ComponentI owner,
			int executorServiceIndex) throws Exception {
		super((Class<? extends OfferedCI>) implementedInterface, owner);
		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * Construit un port entrant MapReduce en précisant uniquement le propriétaire
	 * et l'index du service d'exécution.
	 *
	 * @param executorServiceIndex Index du service d'exécution à utiliser.
	 * @param owner                Composant propriétaire.
	 * @throws Exception En cas d'erreur lors de l'initialisation.
	 */
	public AsynchronousMapReduceInboundPort(int executorServiceIndex, ComponentI owner) throws Exception {
		super(MapReduceCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof MapReduceI);

		assert owner.validExecutorServiceIndex(executorServiceIndex);

		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * Construit un port entrant MapReduce avec une URI, un propriétaire et un index
	 * du service d'exécution.
	 *
	 * @param uri                  URI du port.
	 * @param executorServiceIndex Index du service d'exécution à utiliser.
	 * @param owner                Composant propriétaire.
	 * @throws Exception En cas d'erreur lors de l'initialisation.
	 */
	public AsynchronousMapReduceInboundPort(String uri, int executorServiceIndex, ComponentI owner) throws Exception {
		super(uri, MapReduceCI.class, owner);

		assert uri != null && (owner instanceof MapReduceI);

		assert owner.validExecutorServiceIndex(executorServiceIndex);

		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI#reduce(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A, A,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		this.getOwner().runTask(ThreadsPolicy.MAP_REDUCE_HANDLER_URI, owner -> {
			try {
				((MapReduceI) owner).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, callerNode);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI#clearMapReduceComputation(java.lang.String)
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.getOwner().runTask(ThreadsPolicy.MAP_REDUCE_HANDLER_URI, owner -> {
			try {
				((MapReduceI) owner).clearMapReduceComputation(computationURI);
			} catch (Exception e) {

				e.printStackTrace();
			}
		});

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI#map(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI)
	 */
	@Override
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		this.getOwner().runTask(ThreadsPolicy.MAP_REDUCE_HANDLER_URI, owner -> {
			try {
				((MapReduceI) owner).map(computationURI, selector, processor);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

}
