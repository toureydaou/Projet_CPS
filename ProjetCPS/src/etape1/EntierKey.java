package etape1;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * La classe {@code EntierKey} implémente l'interface {@code ContentKeyI} et représente
 * une clé entière utilisée dans la DHT.
 * 
 * <p><strong>Description</strong></p>
 * 
 * <p>
 *	Elle nous sert de clé pour les données stockées dans la table
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class EntierKey implements ContentKeyI {
	
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;
	private int cle;
	

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------
	
	/**
     * Constructeur de la classe {@code EntierKey}.
     * 
     * @param cle La valeur entière de la clé.
     */
	public EntierKey(int cle) {
		super();
		this.cle = cle;
	}

	/**
     * Retourne la valeur de la clé.
     * 
     * @return La valeur entière de la clé.
     */
	@Override
	public int hashCode() {
		return cle;
	}
	

}
