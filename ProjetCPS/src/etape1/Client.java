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
     * Récupère une donnée à partir de la clé donnée dans la table.
     * 
     * @param key la clé associée à la donnée recherchée.
     * @return la donnée correspondante si elle existe, sinon {@code null}.
     * @throws Exception en cas d'erreur lors de l'opération.
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
     * Insère une paire clé-valeur dans le table.
     * 
     * @param key la clé associée à la donnée.
     * @param value la donnée à insérer.
     * @return la valeur précédemment associée à cette clé, ou {@code null} si elle n'existait pas.
     * @throws Exception en cas d'erreur lors de l'opération.
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
     * Supprime une donnée du DHT à partir de sa clé.
     * 
     * @param key la clé de la donnée à supprimer.
     * @return la donnée supprimée, ou {@code null} si elle n'existait pas.
     * @throws Exception en cas d'erreur lors de l'opération.
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
     * Effectue une opération de type MapReduce sur les données de la table
     * 
     * @param <R> le type des résultats intermédiaires produits par le processeur.
     * @param <A> le type de l'accumulateur utilisé par le réducteur et le combinateur.
     * @param selector le sélecteur utilisé pour choisir les données à traiter.
     * @param processor le processeur appliqué aux données sélectionnées.
     * @param reductor le réducteur utilisé pour agréger les résultats intermédiaires.
     * @param combinator le combinateur utilisé pour fusionner les résultats des réducteurs.
     * @param initialAcc la valeur initiale de l'accumulateur.
     * @return le résultat final de l'opération MapReduce.
     * @throws Exception en cas d'erreur lors de l'opération.
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
