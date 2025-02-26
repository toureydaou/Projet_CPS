package etape1;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

/**
 * La classe {@code EntierKey} implémente l'interface {@code ContentKeyI} et représente
 * une clé entière utilisée dans la DHT.
 */

public class EntierKey implements ContentKeyI {

	private static final long serialVersionUID = 1L;
	private int cle;
	
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
	public int getCle() {
		return cle;
	}
	

}
