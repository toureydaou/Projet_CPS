package etape2.composants;

import java.io.Serializable;

import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

@RequiredInterfaces(required= {DHTServicesCI.class})
public class ClientBCM extends AbstractComponent implements DHTServicesI {

	protected DHTServicesEndPoint dsep;
	
	
	protected ClientBCM(String uri, DHTServicesEndPoint dsep) {
		super(uri, 0, 1);
		this.dsep = dsep;
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.dsep.getClientSideReference().get(key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.dsep.getClientSideReference().put(key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.dsep.getClientSideReference().remove(key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		// TODO Auto-generated method stub
		return this.dsep.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
	}

	
}
