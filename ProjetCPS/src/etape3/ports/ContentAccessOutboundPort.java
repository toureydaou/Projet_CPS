package etape3.ports;

import etape2.ports.ContentAccessSyncOutboundPort;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>ContentAccessSyncOutboundPort</code> implémente un port
 * sortant d'un composant client demandant les services de son interface offerte
 * <code>ContentAccessSyncCI</code> par le serveur. Il contacte donc celui-ci à
 * travers son port sortant en passant part le connecteur.
 * 
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet les composants propriétaires de ce port sont la
 * Facade ainsi que les noeuds.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class ContentAccessOutboundPort extends ContentAccessSyncOutboundPort implements ContentAccessCI {

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
	public ContentAccessOutboundPort(ComponentI owner) throws Exception {
		super(ContentAccessCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade tous deux jouant le role
		// de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ContentAccessOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ContentAccessCI.class, owner);

		assert uri != null && owner != null;
	}

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		((ContentAccessCI)this.getConnector()).get(computationURI, key, caller);;
	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		((ContentAccessCI)this.getConnector()).put(computationURI, key, value, caller);;
		
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		((ContentAccessCI)this.getConnector()).remove(computationURI, key, caller);;
		
	}

	

}
