package etape1;
import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

/**
 * La classe {@code Livre} implémente l'interface {@code ContentDataI} et représente
 * un livre stocké dans la structure.
 */
public class Livre implements ContentDataI {

	private static final long serialVersionUID = 1L;
	public static final String TITRE = "titre";
	public static final String NB_PAGES = "nbPages";
	
	String nomLivre;
	int nbPages;
	
	/**
     * Constructeur de la classe {@code Livre}.
     * 
     * @param nomLivre Le titre du livre.
     * @param nbPages  Le nombre de pages du livre.
     */
	public Livre(String nomLivre, int nbPages) {
		super();
		this.nomLivre = nomLivre;
		this.nbPages = nbPages;
	}
	
	/**
     * Retourne la valeur associée à une propriété donnée.
     * 
     * @param p Le nom de la propriété (ex: TITRE ou NB_PAGES).
     * @return La valeur de la propriété ou {@code null} si la propriété n'existe pas.
     */
	public Serializable getValue(String p) {
		if(p == Livre.TITRE) return nomLivre;
		if(p == Livre.NB_PAGES) return nbPages;
		return null;
	}
	
	

}