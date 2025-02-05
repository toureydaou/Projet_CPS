import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class EntierKey implements ContentKeyI {

	private static final long serialVersionUID = 1L;
	int cle;
	
	public EntierKey(int cle) {
		super();
		this.cle = cle;
	}

	public int getCle() {
		return cle;
	}
	

}
