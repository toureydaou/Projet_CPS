/**
 * @author	Awwal Fagbehouro
 * @author  Touré-Ydaou TEOURI
 */
package etape2.ports;

import java.io.Serializable;


import etape2.composants.FacadeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * 
 */
public class DHTServicesInboundPort extends AbstractInboundPort implements DHTServicesCI {
	
	private static final long serialVersionUID = 1L;

	public DHTServicesInboundPort(ComponentI owner)
			throws Exception {
		super(DHTServicesCI.class, owner);
		
		// le propriétaire de ce port est la facade jouant le role de serveur
		assert	(owner instanceof FacadeBCM);
	}

	public DHTServicesInboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, DHTServicesCI.class, owner);
		
		assert uri != null  &&	(owner instanceof FacadeBCM);
	}
	
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((FacadeBCM) owner).get(key));
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((FacadeBCM) owner).put(key,value));
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((FacadeBCM) owner).remove(key));
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((FacadeBCM) owner).mapReduce(selector, processor, reductor, combinator, initialAcc)
				);
	}

}
