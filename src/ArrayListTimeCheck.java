import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ArrayListTimeCheck {
	
	public static void main(String[] args) {
		List<Integer> value = new ArrayList<Integer>();
		long st = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			value.add(i);
		}
		for (int i = 0; i < value.size(); i++) {
			System.out.println(value.get(i));
		}
		long end = System.currentTimeMillis();
		System.out.println("End Time: " + TimeUnit.MILLISECONDS.toSeconds(end - st));
	}
}
