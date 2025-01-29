
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class Main {
	public static void main(String[] args){
		
		Node[] noeuds = new Node[4];
		for(int i=0; i<4; i++) {
			noeuds[i] = new Node(new IntInterval(i*50, (i+1)*50 -1));
			if(i >0) noeuds[i-1].setSuivant(noeuds[i]);
		}
		noeuds[3].setSuivant(noeuds[0]);

		Facade f = new Facade(noeuds[0]);
		
		try {
			EntierKey k = new EntierKey(100); 
			f.put(k, new Livre("faim", 200));
			f.put(new EntierKey(50), new Livre("soif", 100));
			f.put(new EntierKey(22), new Livre("douleur", 50));
			f.put(new EntierKey(53), new Livre("pitié", 20));
			f.put(new EntierKey(102), new Livre("peur", 50));
			System.out.println(f.get(k).getValue("nbPages"));
			int a = f.mapReduce(i->((int)i.getValue("nbPages"))>0,
					i-> new Livre((String)i.getValue("titre"),(int) i.getValue("nbPages")/2),
					(acc, i) ->  (acc + (int)i.getValue("nbPages")),
					(acc, i) ->  (acc + i),
					0);
			System.out.println(a);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}