package etape4.ports;

import java.io.Serializable;

import etape3.ports.AsynchronousMapReduceInboundPort;
import etape4.policies.ThreadsPolicy;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

//-----------------------------------------------------------------------------
/**
* La classe {@code ParallelMapReduceInboundPort} implémente un port
* entrant pour un composant serveur offrant les services de l'interface
* {@code ParallelMapReduceCI}.
* <p>
* Ce port permet aux clients d'effectuer des opérations de type Map/Reduce sur les composants de
* manière asynchrone en envoyant leurs requêtes au travers d'un
* {@code EndPointI}.
* </p>
* 
* <p>
* Le propriétaire de ce port est un composant jouant le rôle d'un nœud dans un
* système de table de hachage distribuée (DHT) intégrant des fonctionnalités de
* type ParallelMapReduce.
* </p>
* 
* @see ParallelMapReduceCI
* @see AsynchronousMapReduceInboundPort
* 
* @author Touré-Ydaou TEOURI
* @author Awwal FAGBEHOURO
*/
public class ParallelMapReduceInboundPort extends AsynchronousMapReduceInboundPort implements ParallelMapReduceCI {
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
	public ParallelMapReduceInboundPort(int executorServiceIndex, ComponentI owner) throws Exception {
		super(ParallelMapReduceCI.class, owner, executorServiceIndex);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof ParallelMapReduceI);

		assert owner.validExecutorServiceIndex(executorServiceIndex);

	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ParallelMapReduceInboundPort(String uri, int executorServiceIndex, ComponentI owner) throws Exception {
		super(uri, ParallelMapReduceCI.class, owner, executorServiceIndex);

		assert uri != null && (owner instanceof ParallelMapReduceI);

		assert owner.validExecutorServiceIndex(executorServiceIndex);

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI#parallelMap(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI)
	 */
	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		this.getOwner().handleRequest(ThreadsPolicy.MAP_REDUCE_HANDLER_URI, owner -> {
			((ParallelMapReduceI) owner).parallelMap(computationURI, selector, processor, parallelismPolicy);
			return null;
		});

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI#parallelReduce(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A, A, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		this.getOwner().handleRequest(ThreadsPolicy.MAP_REDUCE_HANDLER_URI, owner -> {
			((ParallelMapReduceI) owner).parallelReduce(computationURI, reductor, combinator, identityAcc, currentAcc,
					parallelismPolicy, caller);
			return null;
		});
	}

}
