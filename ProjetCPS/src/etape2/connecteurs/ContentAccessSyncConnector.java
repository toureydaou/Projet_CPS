package etape2.connecteurs;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * La classe <code>ContentAccessSyncConnector</code> implémente un connecteur
 * permettant de connecter le port sortant du client au port entrant du serveur.
 * Il sert de pont pour les appels synchrones aux méthodes d'accès aux contenus.
 *
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class ContentAccessSyncConnector extends AbstractConnector implements ContentAccessSyncCI {

	/**
	 * Invoque la méthode <code>getSync</code> du port entrant du serveur.
	 * 
	 * @param computationURI L'URI de la computation
	 * @param key            La clé pour accéder aux données
	 * @return Le contenu associé à la clé donnée
	 * @throws Exception Si une erreur survient
	 */
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.offering).getSync(computationURI, key);
	}

	/**
	 * Invoque la méthode <code>putSync</code> du port entrant du serveur.
	 * 
	 * @param computationURI L'URI de la computation
	 * @param key            La clé pour insérer les données
	 * @param value          La valeur à insérer
	 * @return Le contenu précédemment associé à la clé donnée
	 * @throws Exception Si une erreur survient
	 */
	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		return ((ContentAccessSyncCI) this.offering).putSync(computationURI, key, value);
	}

	/**
	 * Invoque la méthode <code>removeSync</code> du port entrant du serveur.
	 * 
	 * @param computationURI L'URI de la computation
	 * @param key            La clé pour supprimer les données
	 * @return Le contenu précédemment associé à la clé donnée
	 * @throws Exception Si une erreur survient
	 */
	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.offering).removeSync(computationURI, key);
	}

	/**
	 * Invoque la méthode <code>clearComputation</code> du port entrant du serveur.
	 * 
	 * @param computationURI L'URI de la computation à effacer
	 * @throws Exception Si une erreur survient
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		((ContentAccessSyncCI) this.offering).clearComputation(computationURI);
	}

}
