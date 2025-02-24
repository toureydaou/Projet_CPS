package etape1;

import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class Main {
	public static void main(String[] args) throws ConnectionException{
		
		Node[] noeuds = new Node[4];
		POJOContentNodeCompositeEndPoint[] pojos = new POJOContentNodeCompositeEndPoint[5];
		for(int i = 0; i < pojos.length;i++) {
			pojos[i] = new POJOContentNodeCompositeEndPoint();
		}

		

		noeuds[3] = new Node(new IntInterval(150, 199), pojos[3], pojos[4]);
		noeuds[2] = new Node(new IntInterval(100, 149), pojos[2], pojos[3]);
		noeuds[1] = new Node(new IntInterval(50, 99), pojos[1], pojos[2]);
		noeuds[0] = new Node(new IntInterval(0, 49), pojos[0], pojos[4], pojos[1]);
		
		
		Facade f = new Facade(pojos[0]);
	
		try {
			EntierKey k_22 = new EntierKey(22); 
			EntierKey k_50 = new EntierKey(50); 
			EntierKey k_53 = new EntierKey(53); 
			EntierKey k_102 = new EntierKey(102); 
			EntierKey k_175 = new EntierKey(175); 
			f.put(k_175, new Livre("faim", 200));
			f.put(k_22, new Livre("soif", 100));
			f.put(k_50, new Livre("douleur", 50));
			f.put(k_102, new Livre("pitié", 20));
			System.out.println("Test get, nom livre : " + f.get(k_175).getValue("nbPages"));
			System.out.println("Test put, nom ancien livre : " + f.put(k_102, new Livre("peur", 50)).getValue("titre"));
			System.out.println("Test remove, nom ancien livre : " + f.remove(k_102).getValue("titre"));
			
		
	
			int a = f.mapReduce(i->((int)i.getValue("nbPages"))>0,
					i-> new Livre((String)i.getValue("titre"),(int) i.getValue("nbPages")/2),
					(acc, i) ->  (acc + (int)i.getValue("nbPages")),
					(acc, i) ->  (acc + i),
					0);
			System.out.println("Map reduce " + a);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}