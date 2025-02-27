package etape2.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

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

public class ContentAccessSyncOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {

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
	public ContentAccessSyncOutboundPort(ComponentI owner) throws Exception {
		super(ContentAccessSyncCI.class, owner);

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
	public ContentAccessSyncOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ContentAccessSyncCI.class, owner);

		assert uri != null && owner != null;
	}

	/**
	 * Permet de contacter le service du serveur pour récupérer une donnée dans la
	 * table de hachage en invoquant la méthode <code>getSync</code> du connecteur
	 * <code>ContentAccessSyncConnector</code>
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à récupérer.
	 * @return valeur associée à {@code key} ou {@code null} si la clée est absente.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.getConnector()).getSync(computationURI, key);
	}

	/**
	 * Permet de contacter le service du serveur pour insérer une donnée dans la
	 * table de hachage en invoquant la méthode <code>putSync</code> du connecteur
	 * <code>ContentAccessSyncConnector</code>.
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
		return ((ContentAccessSyncCI) this.getConnector()).putSync(computationURI, key, value);
	}

	/**
	 * Permet de contacter le service du serveur pour supprimer une donnée dans la
	 * table de hachage en invoquant la méthode <code>removeSync</code> du
	 * connecteur <code>ContentAccessSyncConnector</code>
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à supprimer.
	 * @return valeur associée à {@code key} avant suppression
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.getConnector()).removeSync(computationURI, key);
	}

	/**
	 * Permet de nettoyer les données résiliantes sur le serveur en invoquant la
	 * méthode <code>getSync</code> du connecteur
	 * <code>ContentAccessSyncConnector</code>
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à supprimer.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		((ContentAccessSyncCI) this.getConnector()).clearComputation(computationURI);
	}

}
