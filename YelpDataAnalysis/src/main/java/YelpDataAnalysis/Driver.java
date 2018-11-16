package YelpDataAnalysis;

import org.apache.log4j.Logger;
import org.joda.time.LocalDate;

public class Driver {

	//for packaging
	private static final Logger logger = Logger.getLogger(Driver.class);

	public static void main(String[] args) {
		System.out.println(getLocalCurrentDate());
	}

	private static String getLocalCurrentDate() {

		if (logger.isDebugEnabled()) {
			logger.debug("getLocalCurrentDate() is executed!");
		}

		LocalDate date = new LocalDate();
		return date.toString();

	}

}
