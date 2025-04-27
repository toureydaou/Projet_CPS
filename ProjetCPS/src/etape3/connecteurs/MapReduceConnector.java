package etape3.connecteurs;

import java.io.Serializable;

import etape2.connecteurs.MapReduceSyncConnector;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * La classe <code>MapReduceConnector</code> est un connecteur permettant
 * d'invoquer à distance les opérations MapReduce dans un environnement
 * distribué.
 * 
 * <p>
 * Cette classe connecte un client au service MapReduce en transmettant les
 * appels aux méthodes `map`, `reduce` et `clearMapReduceComputation` vers le
 * composant serveur désigné par `offering`.
 * </p>
 * 
 * <p>
 * Hérite de {@link MapReduceSyncConnector} pour ajouter un comportement de
 * connexion synchrone de base.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class MapReduceConnector extends MapReduceSyncConnector implements MapReduceCI {

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
		((MapReduceCI) this.offering).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, callerNode);
	}

	/**
	 * 
	 * @see etape2.connecteurs.MapReduceSyncConnector#clearMapReduceComputation(java.lang.String)
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceCI) this.offering).clearMapReduceComputation(computationURI);
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
		((MapReduceCI) this.offering).map(computationURI, selector, processor);

	}

}
