import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

public class Livre implements ContentDataI {

	private static final long serialVersionUID = 1L;
	
	String nomLivre;
	int nbPages;

	public Livre(String nomLivre, int nbPages) {
		super();
		this.nomLivre = nomLivre;
		this.nbPages = nbPages;
	}

	public Serializable getValue(String p) {
		if(p == "titre") return nomLivre;
		if(p == "nbPages") return nbPages;
		return null;
	}
	
	

}