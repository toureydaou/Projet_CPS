package etape3.ports;

import etape2.ports.ContentAccessSyncOutboundPort;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

//-----------------------------------------------------------------------------
/**
 * La classe {@code AsynchronousContentAccessOutboundPort} implémente un port
 * sortant pour un composant client demandant les services de l'interface
 * {@code ContentAccessCI} auprès d'un composant serveur.
 * 
 * <p>
 * Ce port permet d'envoyer des requêtes d'accès au contenu de manière
 * asynchrone via un connecteur.
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet, les composants propriétaires de ce port sont la
 * {@code Facade} ainsi que les {@code Noeuds} du réseau.
 * </p>
 * 
 * @see fr.sorbonne_u.components.ports.AbstractOutboundPort;
 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
 * 
 * @Author Touré-Ydaou TEOURI
 * @Author Awwal FAGBEHOURO
 */

public class AsynchronousContentAccessOutboundPort extends ContentAccessSyncOutboundPort implements ContentAccessCI {

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
	 * @param owner Composant propriétaire du port
	 * @throws Exception si une erreur survient pendant l'initialisation
	 */
	public AsynchronousContentAccessOutboundPort(ComponentI owner) throws Exception {
		super(ContentAccessCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade tous deux jouant le role
		// de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec une URI spécifique et le composant
	 * propriétaire.
	 *
	 * @param uri   URI unique du port
	 * @param owner Composant propriétaire du port
	 * @throws Exception si une erreur survient pendant l'initialisation
	 */
	public AsynchronousContentAccessOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ContentAccessCI.class, owner);

		assert uri != null && owner != null;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI#get(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		((ContentAccessCI) this.getConnector()).get(computationURI, key, caller);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI#put(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		((ContentAccessCI) this.getConnector()).put(computationURI, key, value, caller);

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI#remove(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		((ContentAccessCI) this.getConnector()).remove(computationURI, key, caller);

	}

}
