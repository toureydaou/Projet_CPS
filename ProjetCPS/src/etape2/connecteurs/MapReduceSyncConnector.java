package etape2.connecteurs;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * La classe <code>MapReduceSyncConnector</code> implémente un connecteur
 * permettant de connecter le port sortant du client au port entrant du serveur.
 * Il sert de pont pour les appels synchrones de map reduce sur la DHT.
 *
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class MapReduceSyncConnector extends AbstractConnector implements MapReduceSyncCI {

	/**
	 * Invoque la méthode <code>mapSync</code> du port entrant du serveur.
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
		((MapReduceSyncCI) this.offering).mapSync(computationURI, selector, processor);
	}

	/**
	 * Invoque la méthode <code>reduceSync</code> du port entrant du serveur.
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
		return ((MapReduceSyncCI) this.offering).reduceSync(computationURI, reductor, combinator, currentAcc);
	}

	/**
	 * Invoque la méthode <code>clearMapReduceComputation</code> du port entrant du
	 * serveur.
	 * 
	 * @param computationURI URI of the computation.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceSyncCI) this.offering).clearMapReduceComputation(computationURI);
	}

}
