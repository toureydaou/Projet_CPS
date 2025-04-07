package etape4.ports;

import java.io.Serializable;

import etape3.ports.AsynchronousMapReduceOutboundPort;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class ParallelMapReduceOutboundPort extends AsynchronousMapReduceOutboundPort implements ParallelMapReduceCI {
	private static final long serialVersionUID = 1L;

	/**
	 * Crée et initialise le port sortant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ParallelMapReduceOutboundPort(ComponentI owner) throws Exception {
		super(ParallelMapReduceCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade jouant le role de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ParallelMapReduceOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ParallelMapReduceCI.class, owner);

		assert uri != null && owner != null;
	}

	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		((ParallelMapReduceI) this.getConnector()).parallelMap(computationURI, selector, processor, parallelismPolicy);
	}

	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		((ParallelMapReduceI) this.getConnector()).parallelReduce(computationURI, reductor, combinator, identityAcc,
				currentAcc, parallelismPolicy, caller);

	}

}
