import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class Facade implements DHTServicesI {
	Node tete;
	
	public Facade(Node tete) {
		this.tete = tete;
		this.tete.enTete = true;
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String uriTete = URIGenerator.generateURI("get");
		return tete.getSync(uriTete, key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String uriTete = URIGenerator.generateURI("put");
		return tete.putSync(uriTete, key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String uriTete = URIGenerator.generateURI("remove");
		return tete.removeSync(uriTete, key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		String uriTete = URIGenerator.generateURI("mapReduce");
		tete.mapSync(uriTete, selector, processor);
		A result = tete.reduceSync(uriTete, reductor, combinator, initialAcc);
		tete.clearMapReduceComputation(uriTete);
		return result;
	}

}