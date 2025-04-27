package etape2.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>DHTServicesOutboundPort</code> implémente un port sortant
 * d'un composant serveur offrant les services de son interface offerte
 * <code>DHTServicesCI</code> le serveur est donc contacté à travers son port
 * entrant.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet le composant propriétaire de ce port est le
 * client.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class DHTServicesOutboundPort extends AbstractOutboundPort implements DHTServicesCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port sortant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public DHTServicesOutboundPort(ComponentI owner) throws Exception {
		super(DHTServicesCI.class, owner);

		// le propriétaire de ce port est un composant jouant le role du client de la
		// facade
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public DHTServicesOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, DHTServicesCI.class, owner);

		assert uri != null && owner != null;
	}

	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI#get(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return ((DHTServicesCI) this.getConnector()).get(key);
	}


	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI#put(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return ((DHTServicesCI) this.getConnector()).put(key, value);
	}

	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI#remove(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return ((DHTServicesCI) this.getConnector()).remove(key);
	}

	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI#mapReduce(fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		return ((DHTServicesCI) this.getConnector()).mapReduce(selector, processor, reductor, combinator, initialAcc);
	}

}
