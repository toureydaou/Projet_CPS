package etape4.policies;


/**
 * La classe {@code ThreadsPolicy} définit les constantes liées
 * à la gestion des threads dans un système utilisant des opérations
 * de type accès au contenu et MapReduce.
 * <p>
 * Elle fournit le nombre de threads alloués pour différentes tâches,
 * notamment l'accès aux contenus et la réception des résultats
 * d'opérations MapReduce.
 * </p>
 * 
 * Constantes :
 * <ul>
 *   <li>{@link #NUMBER_CONTENT_ACCESS_THREADS} : nombre de threads pour accéder aux contenus</li>
 *   <li>{@link #NUMBER_MAP_REDUCE_THREADS} : nombre de threads pour les opérations MapReduce</li>
 *   <li>{@link #NUMBER_MAP_REDUCE_THREADS} : nombre de threads pour les opérations DHTManagement</li>
 *   <li>{@link #NUMBER_ACCEPT_RESULT_CONTENT_ACCESS_THREADS} : nombre de threads pour accepter les résultats d'accès au contenu</li>
 *   <li>{@link #NUMBER_ACCEPT_RESULT_MAP_REDUCE_THREADS} : nombre de threads pour accepter les résultats des opérations MapReduce</li>
 * </ul>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class ThreadsPolicy {
	public static final String RESULT_RECEPTION_HANDLER_URI = "Result-Reception-Content-Access-Pool-Threads";
	public static final String MAP_REDUCE_RESULT_RECEPTION_HANDLER_URI = "Result-Reception-Map-Reduce-Pool-Threads";
	public static final String CONTENT_ACCESS_HANDLER_URI = "Content-Access-Pool-Threads";
	public static final String MAP_REDUCE_HANDLER_URI = "Map-Reduce-Pool-Threads";
	public static final String DHT_MANAGEMENT_HANDLER_URI = "Dht-Management-Pool-Threads";
	
	public static final int NUMBER_CONTENT_ACCESS_THREADS = 20;
	public static final int NUMBER_MAP_REDUCE_THREADS = 20;
	public static final int NUMBER_DHT_MANAGEMEMENT_THREADS = 20;
	public static final int NUMBER_ACCEPT_RESULT_CONTENT_ACCESS_THREADS = 20;
	public static final int NUMBER_ACCEPT_RESULT_MAP_REDUCE_THREADS = 20;

}
