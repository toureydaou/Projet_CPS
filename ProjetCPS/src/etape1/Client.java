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

//-----------------------------------------------------------------------------
/**
 * Classe {@code Client} implémentant l'interface {@code DHTServicesI}.
 * Cette classe agit comme un client pour interagir avec un service de table de hachage distribué (DHT).
 * Elle utilise un {@code POJOEndPoint} pour communiquer avec la façade.
 * 
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class Client implements DHTServicesI {
	
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private POJOEndPoint<DHTServicesI> outboundEndpoint;
	
	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------
	
	 /**
     * Crée un client et lui donne un point de connexion.
     * 
     * @param outboundEndpoint le point de terminaison permettant la communication avec le DHT.
     */
	public Client(POJOEndPoint<DHTServicesI> outboundEndpoint) {
		this.outboundEndpoint = outboundEndpoint;
	}
	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#get(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		// Condition pour vérifier si le serveur est initialisé au préalable
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		return this.outboundEndpoint.getClientSideReference().get(key);
	}
	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#put(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		// Condition pour vérifier si le serveur est initialisé au préalable
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		
		return this.outboundEndpoint.getClientSideReference().put(key, value);
		
	}
	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#remove(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		// Condition pour vérifier si le serveur est initialisé au préalable
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		return this.outboundEndpoint.getClientSideReference().remove(key);
	}
	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#mapReduce(fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		// Condition pour vérifier si le serveur est initialisé au préalable
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		return this.outboundEndpoint.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
	}
}
