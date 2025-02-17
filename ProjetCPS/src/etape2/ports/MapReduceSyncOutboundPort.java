package etape2.ports;

import java.io.Serializable;

import etape2.composants.NodeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class MapReduceSyncOutboundPort extends AbstractOutboundPort implements MapReduceSyncCI {
	
	private static final long serialVersionUID = 1L;

	public MapReduceSyncOutboundPort(ComponentI owner)
			throws Exception {
		super(MapReduceSyncCI.class, owner);
		
		// le propriétaire de ce port est un noeud ou la facade jouant le role de client
		assert	owner != null;
	}

	public MapReduceSyncOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, MapReduceSyncCI.class, owner);
		
		assert uri != null && owner != null;
	}
	
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		((MapReduceSyncCI)this.getConnector()).mapSync(computationURI, selector, processor);
	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		return ((MapReduceSyncCI)this.getConnector()).reduceSync(computationURI, reductor, combinator, currentAcc);
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceSyncCI)this.getConnector()).clearMapReduceComputation(computationURI);
	}

}
