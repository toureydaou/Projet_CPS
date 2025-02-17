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

public class MapReduceSyncInboundPort extends AbstractInboundPort implements MapReduceSyncCI {
	

	private static final long serialVersionUID = 1L;

	public MapReduceSyncInboundPort(ComponentI owner)
			throws Exception {
		super(MapReduceSyncCI.class, owner);
		
		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert	(owner instanceof NodeBCM);
	}

	public MapReduceSyncInboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, MapReduceSyncCI.class, owner);
		
		assert uri != null  &&	(owner instanceof  NodeBCM)  ;
	}
	
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		this.getOwner().handleRequest(owner ->{
			((NodeBCM) owner).mapSync(computationURI, selector, processor);
			return null;
		});
	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		return this.getOwner().handleRequest(owner ->((NodeBCM) owner).reduceSync(computationURI, reductor, combinator, currentAcc));
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.getOwner().handleRequest(owner ->{
			((NodeBCM) owner).clearMapReduceComputation(computationURI);
			return null;
		});
	}

}
