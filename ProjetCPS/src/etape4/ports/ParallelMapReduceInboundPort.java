package etape4.ports;

import java.io.Serializable;

import etape3.ports.AsynchronousMapReduceInboundPort;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

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

	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((ParallelMapReduceI) owner).parallelMap(computationURI, selector, processor, parallelismPolicy);
			return null;
		});

	}

	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((ParallelMapReduceI) owner).parallelReduce(computationURI, reductor, combinator, identityAcc, currentAcc,
					parallelismPolicy, caller);
			return null;
		});
	}

}
