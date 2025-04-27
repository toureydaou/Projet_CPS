package etape4.policies;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI;

/**
 * La classe {@code IgnoreChordsPolicy} implémente une politique de parallélisme
 * pour le service MapReduce sur la DHT (Distributed Hash Table).
 * <p>
 * Cette politique permet d'ignorer un certain nombre de chords (sauts de nœuds
 * dans la DHT) lors de la propagation d'une opération parallèle. Cela est utilisé
 * pour éviter de repasser par des noeuds qui auraient déjà été visités
 * </p>
 * 
 * @see ParallelismPolicyI
 *  
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO 
 */
public class IgnoreChordsPolicy implements ParallelismPolicyI {
	
	private static final long serialVersionUID = 1L;
	public int nbreChordsIgnores;
	
	/**
	 * Crée une nouvelle politique {@code IgnoreChordsPolicy} en spécifiant
	 * le nombre de chords à ignorer.
	 *
	 * @param nbreChordsIgnores le nombre de chords à ignorer lors de la propagation.
	 */
	public IgnoreChordsPolicy(int nbreChordsIgnores) {
		this.nbreChordsIgnores = nbreChordsIgnores;
	}
	
	/**
	 * Retourne le nombre de chords à ignorer selon cette politique.
	 *
	 * @return le nombre de chords ignorés.
	 */
	public int getNbreChordsIgnores() {
		return nbreChordsIgnores;
	}
	
}
