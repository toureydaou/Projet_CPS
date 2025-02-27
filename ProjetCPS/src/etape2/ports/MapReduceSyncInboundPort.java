package etape2.ports;

import java.io.Serializable;

import etape2.composants.NodeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>MapReduceSyncInboundPort</code> implémente un port entrant
 * d'un composant serveur offrant les services de son interface offerte
 * <code>MapReduceSyncCI</code> le serveur est donc contacté à travers son port
 * entrant.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet les composants propriétaires de ce port sont la
 * facade et les noeuds.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class MapReduceSyncInboundPort extends AbstractInboundPort implements MapReduceSyncCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port entrant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceSyncInboundPort(ComponentI owner) throws Exception {
		super(MapReduceSyncCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof NodeBCM);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceSyncInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, MapReduceSyncCI.class, owner);

		assert uri != null && (owner instanceof NodeBCM);
	}

	/**
	 * Invoque la méthode <code>mapSync</code> du serveur.
	 * 
	 * @param <R>            type des résultats de la computation map.
	 * @param computationURI URI de la computation, utilisé pour distinguer les
	 *                       computations parallèles sur la même DHT.
	 * @param selector       une fonction booléenne qui sélectionne les entrées de
	 *                       la DHT à traiter.
	 * @param processor      une fonction implémentant le traitement à appliquer par
	 *                       la map.
	 * @throws Exception <i>à compléter</i>.
	 */
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		this.getOwner().handleRequest(owner -> {
			((NodeBCM) owner).mapSync(computationURI, selector, processor);
			return null;
		});
	}

	/**
	 * Invoque la méthode <code>reduceSync</code> du serveur.
	 * 
	 * @param <R>            type of the results in the map computation.
	 * @param <A>            type of the accumulator in the reduction computation.
	 * @param computationURI URI of the computation, used to distinguish parallel
	 *                       maps over the same DHT.
	 * @param reductor       function {@code A} x {@code R -> A} accumulating one
	 *                       map result.
	 * @param combinator     function {@code A} x {@code A -> A} combining two
	 *                       accumulators.
	 * @param currentAcc     the initial accumulator value.
	 * @return the final accumulator after all reductions.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		return this.getOwner()
				.handleRequest(owner -> ((NodeBCM) owner).reduceSync(computationURI, reductor, combinator, currentAcc));
	}

	/**
	 * Invoque la méthode <code>clearMapReduceComputation</code> du serveur.
	 * 
	 * @param computationURI URI of the computation.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.getOwner().handleRequest(owner -> {
			((NodeBCM) owner).clearMapReduceComputation(computationURI);
			return null;
		});
	}

}
