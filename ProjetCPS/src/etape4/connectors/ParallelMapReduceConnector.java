package etape4.connectors;

import java.io.Serializable;

import etape3.connecteurs.MapReduceConnector;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * Classe {@code ParallelMapReduceConnector} reliant deux composants via l'interface ParallelMapReduceCI.
 * 
 * Elle délègue les appels au composant offrant, permettant ainsi d'exécuter des opérations 
 * de MapReduce en parallèle dans la DHT.
 */
public class ParallelMapReduceConnector extends MapReduceConnector implements ParallelMapReduceCI {

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI#parallelMap(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI)
	 */
	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		((ParallelMapReduceCI) this.offering).parallelMap(computationURI, selector, processor, parallelismPolicy);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI#parallelReduce(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A, A, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		((ParallelMapReduceCI) this.offering).parallelReduce(computationURI, reductor, combinator, identityAcc,
				currentAcc, parallelismPolicy, caller);

	}

}
