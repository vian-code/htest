package uk.co.vianconsulting.hdfs.exception;


public class HDFSIOException extends Exception {

	public HDFSIOException(Exception e) {
		super(e);
	}

	public HDFSIOException(String string) {
		super(string);
	}

}
