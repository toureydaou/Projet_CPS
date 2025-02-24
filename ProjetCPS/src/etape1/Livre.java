package etape1;
import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

public class Livre implements ContentDataI {

	private static final long serialVersionUID = 1L;
	public static final String TITRE = "titre";
	public static final String NB_PAGES = "nbPages";
	
	String nomLivre;
	int nbPages;

	public Livre(String nomLivre, int nbPages) {
		super();
		this.nomLivre = nomLivre;
		this.nbPages = nbPages;
	}

	public Serializable getValue(String p) {
		if(p == Livre.TITRE) return nomLivre;
		if(p == Livre.NB_PAGES) return nbPages;
		return null;
	}
	
	

}