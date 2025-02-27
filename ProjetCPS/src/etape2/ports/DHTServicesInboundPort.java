package etape2.ports;

import java.io.Serializable;

import etape2.composants.FacadeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>DHTServicesInboundPort</code> implémente un port entrant d'un
 * composant serveur offrant les services de son interface offerte
 * <code>DHTServicesCI</code> le serveur est donc contacté à travers son port
 * entrant.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet le composants propriétaire de ce port est la
 * facade.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class DHTServicesInboundPort extends AbstractInboundPort implements DHTServicesCI {

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
	public DHTServicesInboundPort(ComponentI owner) throws Exception {
		super(DHTServicesCI.class, owner);

		// le propriétaire de ce port est la facade jouant le role de serveur
		assert (owner instanceof FacadeBCM);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public DHTServicesInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, DHTServicesCI.class, owner);

		assert uri != null && (owner instanceof FacadeBCM);
	}

	/**
	 * Permet d'appeler le service <code>get</code> de la facade pour déclencher la
	 * récupération d'une donnée dans la table de hachage avec une clée donnée.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à récupérer.
	 * @return valeur associée à {@code key} ou {@code null} si la clée est absente.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(owner -> ((FacadeBCM) owner).get(key));
	}

	/**
	 * Permet d'appeler le service <code>put</code> de la facade pour déclencher
	 * l'insertion d'une donnée dans la table de hachage avec une clée et une valeur
	 * donnée.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à insérer.
	 * @param value           valeur de la donnée à insérer.
	 * @return valeur associée à {@code key} avant l'insersion ou {@code null} si la
	 *         clée est absente.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.getOwner().handleRequest(owner -> ((FacadeBCM) owner).put(key, value));
	}

	/**
	 * Permet d'appeler le service <code>remove</code> de la facade pour déclencher
	 * la suppression d'une donnée dans la table de hachage avec une clée donnée.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à supprimer.
	 * @return valeur associée à {@code key} avant suppression
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(owner -> ((FacadeBCM) owner).remove(key));
	}

	/**
	 * Permet d'appeler le service <code>mapReduce</code> de la facade pour
	 * déclencher un map reduce avec les données de la table de hachage.
	 * 
	 * @param <R>        type du résultat de l'opération du map.
	 * @param <A>        type de l'accumulateur de l'opération du reduce.
	 * @param selector   fonction booléenne qui filtre les données de la table de
	 *                   hachage qui seront traitées par la map.
	 * @param processor  fonction implémentant le traitement à appliquer par la map
	 *                   elle-même.
	 * @param reductor   fonction {@code A} x {@code R -> A} accumulant un résultat
	 *                   de la map.
	 * @param combinator fonction {@code A} x {@code A -> A} combinant deux
	 *                   accumulateurs.
	 * @param initialAcc valeur initiale de l'accumulateur.
	 * @return valeur finale de l'accumulateur après toutes les reduces.
	 */
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((FacadeBCM) owner).mapReduce(selector, processor, reductor, combinator, initialAcc));
	}

}
