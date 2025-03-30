package etape3.composants;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape1.EntierKey;
import etape1.Livre;
import etape2.endpoints.DHTServicesEndPoint;
import etape3.CVM;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.utils.aclocks.AcceleratedClock;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;

/**
 * ClientBCM est un composant client qui interagit avec le service DHT pour
 * effectuer des opérations de type MapReduce, ainsi que des opérations CRUD sur
 * les données stockées dans le DHT.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ClientBCM extends AbstractComponent {

	protected DHTServicesEndPoint endPointClientFacade; // Point d'accès aux services DHT
    protected AcceleratedClock dhtClock;  // Référence à l'horloge

	private static final int SCHEDULABLE_THREADS = 2;
	private static final int THREADS_NUMBER = 0;

	/**
	 * Constructeur du composant ClientBCM.
	 * 
	 * @param uri                  URI du composant.
	 * @param endpointClientFacade Point d'accès aux services DHT.
	 */
	protected ClientBCM(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointClientFacade = endpointClientFacade;

	}
	
	protected void connectToClockServer() throws Exception {
	    ClocksServerOutboundPort p = new ClocksServerOutboundPort(this);
	    p.publishPort();
	    
	    this.doPortConnection(
	        p.getPortURI(),
	        ClocksServer.STANDARD_INBOUNDPORT_URI,
	        ClocksServerConnector.class.getCanonicalName()
	    );
	    
	    this.dhtClock = p.getClock(CVM.TEST_CLOCK_URI);
	    
	    this.doPortDisconnection(p.getPortURI());
	    p.unpublishPort();
	    p.destroyPort();
	    
	    this.logMessage("En attente du démarrage de l'horloge...");
	    dhtClock.waitUntilStart();
	    this.logMessage("Horloge démarrée : " + dhtClock.getStartInstant());
	}

	/**
	 * Récupère une donnée à partir de la clé fournie.
	 * 
	 * @param key Clé de la donnée à récupérer.
	 * @return La donnée correspondante à la clé.
	 * @throws Exception Si une erreur se produit lors de la récupération.
	 */
	public ContentDataI get(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'GET' sur la facade");
		return this.endPointClientFacade.getClientSideReference().get(key);
	}

	/**
	 * Ajoute ou met à jour une donnée associée à une clé dans le DHT.
	 * 
	 * @param key   La clé associée à la donnée.
	 * @param value La donnée à stocker.
	 * @return La donnée précédente associée à la clé (ou null si c'est un nouvel
	 *         élément).
	 * @throws Exception Si une erreur se produit lors de l'ajout.
	 */
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		System.out.println("Envoi de la requête 'PUT' sur la facade");
		return this.endPointClientFacade.getClientSideReference().put(key, value);
	}

	/**
	 * Supprime une donnée associée à une clé dans le DHT.
	 * 
	 * @param key La clé associée à la donnée à supprimer.
	 * @return La donnée supprimée.
	 * @throws Exception Si une erreur se produit lors de la suppression.
	 */
	public ContentDataI remove(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'REMOVE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().remove(key);
	}

	/**
	 * Effectue une opération MapReduce sur les données stockées dans le DHT.
	 * 
	 * @param selector   Un sélecteur pour filtrer les données.
	 * @param processor  Un processeur pour transformer les données.
	 * @param reductor   Un réducteur pour agréger les résultats.
	 * @param combinator Un combinator pour combiner les résultats intermédiaires.
	 * @param initialAcc L'accumulateur initial.
	 * @return Le résultat final de l'opération MapReduce.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		System.out.println("Envoi de la requête 'MAP REDUCE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().mapReduce(selector, processor, reductor, combinator,
				initialAcc);
	}

	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting client component.");
		try {
			
			this.connectToClockServer();
			
			if (!endPointClientFacade.clientSideInitialised()) {
				this.endPointClientFacade.initialiseClientSide(this);
			}
			
			super.start();

		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component." + isStarted());
		
		Instant i0 = dhtClock.getStartInstant();
		Instant i1 = i0.plusSeconds(60);
		
		long delay = dhtClock.nanoDelayUntilInstant(i1);
		
		this.scheduleTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {

					    // Test 1: Insertion de données
					    testInsertion();
					    
					    // Test 2: Récupération de données
					    testRecuperation();
					    
					    // Test 3: MapReduce
					    testMapReduce();
					    
					    // Test 4: Suppression de données
					    testSuppression();
					    
				} catch (Exception e) {
					e.printStackTrace();
				} finally {

				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

	//--------------------------------------------------------
	// Méthodes de test
	//--------------------------------------------------------

	/**
	 * Teste l'insertion de données dans le DHT.
	 */
	protected void testInsertion() throws Exception {
	    System.out.println("\n=== TEST INSERTION ===");
	    
	    
	    System.out.println("Client "  + this.reflectionInboundPortURI);
	    
	    EntierKey k10 = new EntierKey(10);
	    EntierKey k20 = new EntierKey(11);
	    EntierKey k30 = new EntierKey(12);
	    EntierKey k40 = new EntierKey(13);
	    EntierKey k50 = new EntierKey(14);
	    EntierKey k60 = new EntierKey(15);
	    EntierKey k70 = new EntierKey(16);
	    EntierKey k80 = new EntierKey(17);
	    EntierKey k90 = new EntierKey(18);
	    EntierKey k100 = new EntierKey(19);
	    EntierKey k120 = new EntierKey(20);
	    EntierKey k130 = new EntierKey(21);
	    
	    Livre livreHP_10 = new Livre("Harry Potter", 200);
	    
	    Livre livreHP_20 = new Livre("Harry Potter", 400);
	    Livre livreHP_new_20 = new Livre("Harry Potter", 500);
	
	    Livre livreHP_new_30 = new Livre("Harry Potter", 700);

	    Livre livreHP_new_40 = new Livre("Harry Potter", 900);
	   
	    Livre livreHP_new_50 = new Livre("Harry Potter", 1100);
	   
	    Livre livreHP_new_60 = new Livre("Harry Potter", 1300);
	    
	    Livre livreHP_new_70 = new Livre("Harry Potter", 1500);
	    
	    Livre livreHP_new_80 = new Livre("Harry Potter", 1700);
	   
	    Livre livreHP_new_90 = new Livre("Harry Potter", 1900);
	  
	    Livre livreHP_new_100 = new Livre("Harry Potter", 2100);
	
	    Livre livreHP_new_120 = new Livre("Harry Potter", 2300);

	    Livre livreHP_new_130 = new Livre("Harry Potter", 2300);
	    
	    
	    System.out.println("Insertion de la clé 10 avec valeur: " + livreHP_10);
	    ContentDataI previousValue_10 = this.put(k10, livreHP_10);
	    this.put(k20, livreHP_10);
	    ContentDataI previousValue_20 = this.put(k20, livreHP_new_20);
	    this.put(k30, livreHP_20);
	    ContentDataI previousValue_30 = this.put(k30, livreHP_new_30);
	    this.put(k40, livreHP_20);
	    ContentDataI previousValue_40 = this.put(k40, livreHP_new_40);
	    this.put(k50, livreHP_20);
	    ContentDataI previousValue_50 = this.put(k50, livreHP_new_50);
	    this.put(k60, livreHP_20);
	    ContentDataI previousValue_60 = this.put(k60, livreHP_new_60);
	    this.put(k70, livreHP_20);
	    ContentDataI previousValue_70 = this.put(k70, livreHP_new_70);
	    this.put(k80, livreHP_20);
	    ContentDataI previousValue_80 = this.put(k80, livreHP_new_80);
	    this.put(k90, livreHP_20);
	    ContentDataI previousValue_90 = this.put(k90, livreHP_new_90);
	    this.put(k100, livreHP_20);
	    ContentDataI previousValue_100 = this.put(k100, livreHP_new_100);
	    this.put(k120, livreHP_20);
	    ContentDataI previousValue_120 = this.put(k120, livreHP_new_120);
	    this.put(k130, livreHP_20);
	    ContentDataI previousValue_130 = this.put(k130, livreHP_new_130);
	    
	    
	    System.out.println("Résultat attendu: null (première insertion)");
	    System.out.println("Résultat obtenu: " + previousValue_10);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_20);

	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_30);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_40);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_50);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_60);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_70);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_80);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_90);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_100);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_120);
	    
	    System.out.println("Résultat attendu: Livre[Harry Potter, 400]");
	    System.out.println("Résultat obtenu: " + previousValue_130);
	    
	    // Vérification
	    if (previousValue_10 != null) {
	        System.err.println("ERREUR: La valeur précédente devrait être null");
	    }
	}

	/**
	 * Teste la récupération de données depuis le DHT.
	 */
	protected void testRecuperation() throws Exception {
	    System.out.println("\n=== TEST RECUPERATION ===");
	    
	    System.out.println("Client "  + this.reflectionInboundPortURI);
	    
	    EntierKey k10 = new EntierKey(10);
	    System.out.println("Récupération de la clé 10");
	    
	    ContentDataI value = this.get(k10);
	    System.out.println("Résultat attendu: Livre[Harry Potter, 200]");
	    System.out.println("Résultat obtenu: " + value);
	}

	/**
	 * Teste l'opération MapReduce.
	 */
	protected void testMapReduce() throws Exception {
	    System.out.println("\n=== TEST MAPREDUCE ===");
	    
	    System.out.println("Client "  + this.reflectionInboundPortURI);
	    
	    
	    EntierKey k_10 = new EntierKey(10);
		EntierKey k_50 = new EntierKey(50);
		EntierKey k_100 = new EntierKey(100);
		
	
		this.put(k_10, new Livre("Harry potter", 200));
		this.put(k_50, new Livre("Harry potter", 200));
		this.put(k_100, new Livre("Harry potter", 200));
	    
	    System.out.println("Calcul du total des pages de tous les livres");
	    int totalPages = this.mapReduce(
	        i -> ((int) i.getValue(Livre.NB_PAGES)) > 0,
	        i -> new Livre((String) i.getValue(Livre.TITRE), (int) i.getValue(Livre.NB_PAGES)),
	        (acc, i) -> acc + (int) i.getValue(Livre.NB_PAGES),
	        (acc1, acc2) -> acc1 + acc2,
	        0
	    );
	    
	    System.out.println("Résultat attendu: 600 (200 pages x 3 livres insérés)");
	    System.out.println("Résultat obtenu: " + totalPages);
	    
	    // Vérification
	    if (totalPages != 600) {
	        System.err.println("ERREUR: Le calcul MapReduce est incorrect");
	    }
	}

	/**
	 * Teste la suppression de données dans le DHT.
	 */
	protected void testSuppression() throws Exception {
	    System.out.println("\n=== TEST SUPPRESSION ===");
	    
	    System.out.println("Client "  + this.reflectionInboundPortURI);
	    
	    EntierKey k10 = new EntierKey(10);
	    System.out.println("Suppression de la clé 10");
	    
	    ContentDataI deletedValue = this.remove(k10);
	    System.out.println("Résultat attendu: Livre[Harry Potter, 200]");
	    System.out.println("Résultat obtenu: " + deletedValue);
	    
	    // Vérification post-suppression
	    ContentDataI shouldBeNull = this.get(k10);
	    System.out.println("Vérification post-suppression (devrait être null): " + shouldBeNull);
	    
	    if (shouldBeNull != null) {
	        System.err.println("ERREUR: La valeur n'a pas été correctement supprimée");
	    }
	}

	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping client component.");
		this.printExecutionLogOnFile("client");
		this.endPointClientFacade.cleanUpClientSide();
		super.finalise();
	}

}
