package etape1;

import java.io.Serializable;

import fr.sorbonne_u.components.endpoints.POJOEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class Client implements DHTServicesI {

	private POJOEndPoint<DHTServicesI> outboundEndpoint;
	
	public Client(POJOEndPoint<DHTServicesI> outboundEndpoint) {
		this.outboundEndpoint = outboundEndpoint;
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		return this.outboundEndpoint.getClientSideReference().get(key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		
		return this.outboundEndpoint.getClientSideReference().put(key, value);
		
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		return this.outboundEndpoint.getClientSideReference().remove(key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		return this.outboundEndpoint.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
	}
}
