package etape2.ports;

import etape2.composants.NodeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>ContentAccessSyncInboundPort</code> implémente un port
 * entrant d'un composant serveur offrant les services de son interface offerte
 * <code>ContentAccessSyncCI</code> le serveur est donc contacté à travers son
 * port entrant.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet les composants propriétaires de ce port sont les
 * noeuds.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class ContentAccessSyncInboundPort extends AbstractInboundPort implements ContentAccessSyncCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port entrant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ContentAccessSyncInboundPort(ComponentI owner) throws Exception {
		super(ContentAccessSyncCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof NodeBCM);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ContentAccessSyncInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ContentAccessSyncCI.class, owner);

		assert uri != null && (owner instanceof NodeBCM);
	}
	
	
	
	
	public ContentAccessSyncInboundPort(Class<? extends OfferedCI> implementedInterface, ComponentI owner) throws Exception {
		super(implementedInterface, owner);	
	}
	
	
	public ContentAccessSyncInboundPort(String uri, Class<? extends OfferedCI> implementedInterface, ComponentI owner) throws Exception {
		super(uri, implementedInterface, owner);	
	}
	
	

	/**
	 * Permet d'appeler le service <code>getSync</code> du composant pour récupérer
	 * une donnée dans la table de hachage.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à récupérer.
	 * @return valeur associée à {@code key} ou {@code null} si la clée est absente.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {

		return this.getOwner().handleRequest(owner -> ((NodeBCM) owner).getSync(computationURI, key));
	}

	/**
	 * Permet d'appeler le service <code>putSync</code> du composant pour insérer
	 * une donnée dans la table de hachage.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à insérer.
	 * @param value           valeur de la donnée à insérer.
	 * @return valeur associée à {@code key} avant l'insersion ou {@code null} si la
	 *         clée est absente.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		return this.getOwner().handleRequest(owner -> ((NodeBCM) owner).putSync(computationURI, key, value));
	}

	/**
	 * Permet d'appeler le service <code>removeSync</code> du composant pour
	 * supprimer une donnée dans la table de hachage.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à supprimer.
	 * @return valeur associée à {@code key} avant suppression.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(owner -> ((NodeBCM) owner).removeSync(computationURI, key));
	}

	/**
	 * Permet d'appeler le service <code>clearComputation</code> du composant pour
	 * nettoyer les données résiliantes sur le serveur.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		this.getOwner().handleRequest(owner -> {
			((NodeBCM) owner).clearComputation(computationURI);
			return null;
		});
	}

}
