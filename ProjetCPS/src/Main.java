
public class Main {
	public static void main(String[] args){
		Facade f = new Facade(3);
		
		try {
			EntierKey k = new EntierKey(100); 
			f.put(k, new Livre("faim"));
			f.get(k);
			System.out.println(f.remove(k).getValue("titre"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
