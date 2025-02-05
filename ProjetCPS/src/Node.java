import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class Node implements ContentAccessSyncI,MapReduceSyncI{
	HashMap<ContentKeyI, ContentDataI> content;
	IntInterval intervalle;
	ArrayList<String> uriPassages = new ArrayList<>();
	HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();
	POJOContentNodeCompositeEndPoint connexionSortante;
	
	
	public HashMap<ContentKeyI, ContentDataI> getContent() {
		return content;
	}



	public Node(IntInterval intervalle, POJOContentNodeCompositeEndPoint connexionEntrante, POJOContentNodeCompositeEndPoint connexionSortante) {
		this.content = new HashMap<ContentKeyI, ContentDataI>();
		this.intervalle = intervalle;
		this.connexionSortante = connexionSortante;
		connexionEntrante.initialiseServerSide(this);
		connexionSortante.initialiseClientSide(connexionSortante);
	}
	
	public Node(IntInterval intervalle, POJOContentNodeCompositeEndPoint connexionEntrante1,  POJOContentNodeCompositeEndPoint connexionEntrante2, POJOContentNodeCompositeEndPoint connexionSortante) {
		this.content = new HashMap<ContentKeyI, ContentDataI>();
		this.intervalle = intervalle;
		this.connexionSortante = connexionSortante;
		connexionEntrante1.initialiseServerSide(this);
		connexionEntrante2.initialiseServerSide(this);
		connexionSortante.initialiseClientSide(connexionSortante);
	}

	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		if(uriPassages.contains(computationURI)) return;
		uriPassages.add(computationURI);
		memory.put(computationURI, (Stream<ContentDataI>) content.values().stream()
				.filter(selector)
				.map(processor));
		this.connexionSortante.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		if(!uriPassages.contains(computationURI)) return currentAcc;
		uriPassages.remove(computationURI);
		
		return combinator.apply(memory.get(computationURI)
				.reduce(currentAcc,(u,d)-> reductor.apply(u,(R) d), combinator),
				this.connexionSortante.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI, reductor, combinator, currentAcc));
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if(uriPassages.contains(computationURI)) return;
		uriPassages.add(computationURI);
		memory.remove(computationURI);
		this.connexionSortante.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		if(uriPassages.contains(computationURI)) return null;
		uriPassages.add(computationURI);
		int n = ((EntierKey) key).getCle();		
		if(intervalle.in(n)) {
			return content.get(key);
		}
		return this.connexionSortante.getContentAccessEndpoint().getClientSideReference().getSync(computationURI,key);
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		if(uriPassages.contains(computationURI)) return null;
		uriPassages.add(computationURI);
		int n = ((EntierKey) key).getCle();	
		if(intervalle.in(n)) {
			ContentDataI valuePrec = content.get(key);
			this.content.put(key, value);
			return valuePrec;
		}
		return this.connexionSortante.getContentAccessEndpoint().getClientSideReference().putSync(computationURI, key, value);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		if(uriPassages.contains(computationURI)) return null;
		uriPassages.add(computationURI);
		int n = ((EntierKey) key).getCle();	
		if(intervalle.in(n)) {
			ContentDataI valuePrec = content.get(key);
			this.content.remove(key);
			return valuePrec;
		}
		return this.connexionSortante.getContentAccessEndpoint().getClientSideReference().removeSync(computationURI, key);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		if(!uriPassages.contains(computationURI)) return;
		uriPassages.remove(computationURI);
		this.connexionSortante.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);

		
		
	}

	
	
	

}