package etape1;

import java.io.Serializable;

import fr.sorbonne_u.components.endpoints.POJOEndPoint;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * La classe {@code Facade} implémente l'interface {@code DHTServicesI} et
 * fournit des méthodes pour interagir avec les noeuds et exécuter des
 * opérations.
 * 
 * <p><strong>Description</strong></p>
 * 
 * <p>
 *	Les facade sert de frontend à notre architecture. Elle permet de faire le 
 * lien avec les données stockées dans les noeuds.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */


public class Facade implements DHTServicesI {
	
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final String GET_URL = "GET";
	private static final String PUT_URL = "PUT";
	private static final String REMOVE_URL = "REMOVE";
	private static final String MAPREDUCE_URL = "MAPREDUCE";

	// point de connexion de la facade avec les noeuds
	POJOContentNodeCompositeEndPoint outboundEndpoint;
	// point de connexion de la facade avec le client
	POJOEndPoint<DHTServicesI> inboundEndpoint;
	

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Constructeur de la classe {@code Facade}.
	 * 
	 * @param connexion Instance de {@code POJOContentNodeCompositeEndPoint}
	 *                  utilisée pour la connexion avec le premier noeud.
	 * @param connexion Instance de {@code POJOEndPoint<DHTServicesI>}
	 *                  utilisée pour la connexion avec le client.
	 * @throws ConnectionException Si une erreur de connexion survient.
	 */
	public Facade(POJOContentNodeCompositeEndPoint outboundEndpoint, POJOEndPoint<DHTServicesI> inboundEndpoint)
			throws ConnectionException {
		this.outboundEndpoint = outboundEndpoint;
		this.inboundEndpoint = inboundEndpoint;
		this.inboundEndpoint.initialiseServerSide(this);

	}

	/**
	 * Récupère une donnée associée à une clé donnée dans la DHT.
	 * 
	 * @param key La clé associée aux données à récupérer.
	 * @return Les données associées à la clé.
	 * @throws Exception Si une erreur survient lors de la récupération.
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		String uriTete = URIGenerator.generateURI(GET_URL);
		ContentDataI data = this.outboundEndpoint.getContentAccessEndpoint().getClientSideReference().getSync(uriTete, key);
		this.outboundEndpoint.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return data;
	}

	/**
	 * Insère une donnée associée à une clé dans la DHT.
	 * 
	 * @param key   La clé sous laquelle la donnée sera stockée.
	 * @param value La donnée à stocker.
	 * @return La donnée stockée.
	 * @throws Exception Si une erreur survient lors de l'insertion.
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		String uriTete = URIGenerator.generateURI(PUT_URL);
		ContentDataI data = this.outboundEndpoint.getContentAccessEndpoint().getClientSideReference().putSync(uriTete, key,
				value);
		this.outboundEndpoint.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return data;
	}

	/**
	 * Supprime une donnée associée à une clé de la DHT.
	 * 
	 * @param key La clé de la donnée à supprimer.
	 * @return La donnée supprimée.
	 * @throws Exception Si une erreur survient lors de la suppression.
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		String uriTete = URIGenerator.generateURI(REMOVE_URL);
		ContentDataI data = this.outboundEndpoint.getContentAccessEndpoint().getClientSideReference().removeSync(uriTete, key);
		this.outboundEndpoint.getContentAccessEndpoint().getClientSideReference().clearComputation(uriTete);
		return data;
	}

	/**
	 * Exécute une opération MapReduce sur les données stockées dans les noeuds.
	 * 
	 * @param <R>        Le type des résultats intermédiaires.
	 * @param <A>        Le type du résultat final.
	 * @param selector   Le sélecteur utilisé pour filtrer les données.
	 * @param processor  Le processeur appliqué aux données sélectionnées.
	 * @param reductor   Le réducteur qui combine les résultats intermédiaires.
	 * @param combinator Le combinateur qui fusionne les résultats réduits.
	 * @param initialAcc L'accumulateur initial pour la réduction.
	 * @return Le résultat final après l'exécution du MapReduce.
	 * @throws Exception Si une erreur survient lors de l'exécution du MapReduce.
	 */
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		if (!outboundEndpoint.clientSideInitialised()) {
			System.out.println("Serveur non initialisé ");
			outboundEndpoint.initialiseClientSide(outboundEndpoint);
		}
		String uriTete = URIGenerator.generateURI(MAPREDUCE_URL);
		this.outboundEndpoint.getMapReduceEndpoint().getClientSideReference().mapSync(uriTete, selector, processor);
		A result = this.outboundEndpoint.getMapReduceEndpoint().getClientSideReference().reduceSync(uriTete, reductor,
				combinator, initialAcc);
		this.outboundEndpoint.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(uriTete);
		return result;
	}

}