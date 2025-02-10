package etape1;
import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class Facade implements DHTServicesI {
	
	private static final String GET_URL = "GET";
	private static final String PUT_URL = "PUT";
	private static final String REMOVE_URL = "REMOVE";
	private static final String MAPREDUCE_URL = "MAPREDUCE";
	
	POJOContentNodeCompositeEndPoint connexion = new POJOContentNodeCompositeEndPoint();
	
	public Facade(POJOContentNodeCompositeEndPoint connexion) {
		this.connexion = connexion;
		connexion.initialiseClientSide(connexion);
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String uriTete = URIGenerator.generateURI(GET_URL);
		ContentDataI data = this.connexion.getContentAccessEndpoint().getClientSideReference().getSync(uriTete, key);
		this.connexion.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return data;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String uriTete = URIGenerator.generateURI(PUT_URL);
		ContentDataI data = this.connexion.getContentAccessEndpoint().getClientSideReference().putSync(uriTete, key,value);
		this.connexion.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return data;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String uriTete = URIGenerator.generateURI(REMOVE_URL);
		ContentDataI data = this.connexion.getContentAccessEndpoint().getClientSideReference().getSync(uriTete, key);
		this.connexion.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return data;
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		String uriTete = URIGenerator.generateURI(MAPREDUCE_URL);
		this.connexion.getMapReduceEndpoint().getClientSideReference().mapSync(uriTete, selector, processor);
		A result = this.connexion.getMapReduceEndpoint().getClientSideReference().reduceSync(uriTete, reductor, combinator, initialAcc);
		this.connexion.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(uriTete);
		this.connexion.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return result;
	}

}