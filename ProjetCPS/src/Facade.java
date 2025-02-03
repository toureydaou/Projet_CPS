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
	POJOContentNodeCompositeEndPoint connexion = new POJOContentNodeCompositeEndPoint();
	
	public Facade(POJOContentNodeCompositeEndPoint connexion) {
		this.connexion = connexion;
		connexion.initialiseClientSide(connexion);
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String uriTete = URIGenerator.generateURI("get");
		return this.connexion.getContentAccessEndpoint().getClientSideReference().getSync(uriTete, key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String uriTete = URIGenerator.generateURI("put");

		return this.connexion.getContentAccessEndpoint().getClientSideReference().putSync(uriTete, key,value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String uriTete = URIGenerator.generateURI("remove");
		return this.connexion.getContentAccessEndpoint().getClientSideReference().getSync(uriTete, key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		String uriTete = URIGenerator.generateURI("mapReduce");
		this.connexion.getMapReduceEndpoint().getClientSideReference().mapSync(uriTete, selector, processor);
		A result = this.connexion.getMapReduceEndpoint().getClientSideReference().reduceSync(uriTete, reductor, combinator, initialAcc);
		this.connexion.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(uriTete);
		return result;
	}

}