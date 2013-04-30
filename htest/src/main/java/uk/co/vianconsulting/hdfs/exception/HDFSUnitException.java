package uk.co.vianconsulting.hdfs.exception;



public class HDFSUnitException extends Exception {

	public HDFSUnitException(String string) {
		super(string);
	}

	public HDFSUnitException(String string, Exception e) {
		super(string,e);
	}

}
