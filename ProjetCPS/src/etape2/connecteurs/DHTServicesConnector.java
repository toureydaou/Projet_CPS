package etape2.connecteurs;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * La classe <code>DHTServicesConnector</code> implémente un connecteur
 * permettant de connecter le port sortant du client au port entrant du serveur.
 * Il sert de pont pour les appels synchrones de map reduce sur la DHT et
 * d'accès au contenu de la DHT.
 *
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class DHTServicesConnector extends AbstractConnector implements DHTServicesCI {

	/**
	 * Permet d'appeler le service <code>get</code> du port entrant de la facade
	 * pour déclencher la récupération d'une donnée dans la table de hachage avec
	 * une clée donnée.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à récupérer.
	 * @return valeur associée à {@code key} ou {@code null} si la clée est absente.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return ((DHTServicesCI) this.offering).get(key);
	}

	/**
	 * Permet d'appeler le service <code>put</code> du port entrant de la facade
	 * pour déclencher l'insertion d'une donnée dans la table de hachage avec une
	 * clée et une valeur donnée.
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
		return ((DHTServicesCI) this.offering).put(key, value);
	}

	/**
	 * Permet d'appeler le service <code>remove</code> du port entrant facade pour
	 * déclencher la suppression d'une donnée dans la table de hachage avec une clée
	 * donnée.
	 * 
	 * @param compoutationURI URI de la requete.
	 * @param key             clée de la donnée à supprimer.
	 * @return valeur associée à {@code key} avant suppression
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return ((DHTServicesCI) this.offering).remove(key);
	}

	/**
	 * Permet d'appeler le service <code>mapReduce</code> du port entrant la facade
	 * pour déclencher un map reduce avec les données de la table de hachage.
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
		return ((DHTServicesCI) this.offering).mapReduce(selector, processor, reductor, combinator, initialAcc);
	}

}
