package uk.co.vianconsulting.hdfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import junit.framework.AssertionFailedError;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

import uk.co.vianconsulting.hdfs.exception.HDFSUnitException;

public class HDFSUnitTest {
	private static FileSystem fileSystem;
	private static boolean setup;
	private static String testBuffer;
	private static Object currentPath;

	/**
	 * Initializes the test case with settings in a Hadoop configuration file.
	 * 
	 * @param configuration
	 * @throws HDFSUnitException
	 */
	private static void setup(String configuration) throws HDFSUnitException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(configuration));
		try {
			fileSystem = FileSystem.get(conf);
			setup = true;
		} catch (IOException e) {
			throw new HDFSUnitException("Failed to initialize test case", e);
		}
	}

	public static boolean assertFileExists(Path path) throws HDFSUnitException {
		checkSetup();
		try {
			if (!fileSystem.exists(path)) {
				fail("Expected path does not exist: " + path.toString());
			}
		} catch (IOException e) {
			throw new HDFSUnitException("Failed to assert whether file exists",
					e);
		}
		return true;

	}

	public static boolean assertPermissions(Path path, FsAction expectedAction)
			throws HDFSUnitException {
		checkSetup();
		try {
			if (!fileSystem.getFileStatus(path).getPermission().getUserAction()
					.implies(expectedAction)) {
				fail("Path: " + path.toString() + " does not have permission "
						+ expectedAction);
			}
		} catch (IOException e) {
			throw new HDFSUnitException("Failed to assert file permissions: ",
					e);
		}
		return true;

	}

	/**
	 * Asserts whether a given file has a number of lines.
	 * 
	 * @param path
	 * @param expectedLines
	 * @throws HDFSUnitException
	 */
	public static boolean assertNumberOfLines(Path path, int expectedLines)
			throws HDFSUnitException {
		checkAndFillBuffer(path);
		int actualLength = testBuffer.split("\n").length;
		if (actualLength != expectedLines) {
			fail("Expected path: " + path + " to contain " + expectedLines
					+ "lines but it contained" + actualLength);
		}
		return true;
	}

	public static boolean assertLinesEqualText(Path path, String... lines)
			throws HDFSUnitException {
		checkAndFillBuffer(path);
		String[] splitBuffer = splitIntoLineArray(testBuffer);
		if (lines.length > splitBuffer.length) {
			fail("There are more expected lines: " + lines.length
					+ "than actually exist in the path" + path.toString());
		}
		for (int i = 0; i < lines.length; i++) {
			if (!splitBuffer[i].equals(lines[i])) {
				fail("Expected line (" + i + "):\n " + lines[i]
						+ " does not equal actual line (" + i + "):\n"
						+ splitBuffer[i]);
			}
		}

		return true;
	}

	public static boolean assertLinesEqualText(Path path, String localInputPath)
			throws HDFSUnitException {
		checkAndFillBuffer(path);
		assertLinesEqualText(path, readLocalFileIntoArray(localInputPath));
		return true;
	}

	private static String[] readLocalFileIntoArray(String localInputPath)
			throws HDFSUnitException {
		BufferedReader br = null;
		ArrayList<String> lines = new ArrayList<String>();
		try {

			String currentLine;
			br = new BufferedReader(new FileReader(localInputPath));
			while ((currentLine = br.readLine()) != null) {
				lines.add(currentLine);
			}
			return lines.toArray(new String[] {});
		} catch (IOException e) {
			throw new HDFSUnitException(
					"Failed to read file:" + localInputPath, e);
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				throw new HDFSUnitException("Failed to read file:"
						+ localInputPath, e);
			}
		}
	}

	private static String[] splitIntoLineArray(String input) {
		return input.split("\n");
	}

	private static void checkAndFillBuffer(Path path) throws HDFSUnitException {
		assertFileExists(path);
		if (currentPath == null || currentPath != path) {
			testBuffer = readHDFSFileToString(path);
			currentPath = path;
		}

	}

	private static String readHDFSFileToString(Path path)
			throws HDFSUnitException {
		checkSetup();
		FSDataInputStream inputStream = null;
		try {
			assertFileExists(path);
			inputStream = fileSystem.open(path);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					inputStream));
			String line = null;
			String output = "";
			while (((line = reader.readLine()) != null)) {

				output += line + "\n";
			}
			return output;

		} catch (IOException e) {
			throw new HDFSUnitException("Failed to read file: "
					+ path.toString(), e);
		} finally {
			try {

				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				throw new HDFSUnitException("Failed to read file: "
						+ path.toString(), e);
			}
		}
	}

	private static void checkSetup() throws HDFSUnitException {
		if (!setup) {
			throw new HDFSUnitException(
					"Initialize the test case first. Call setup() with a configuration file");
		}
	}

	private static void fail(String message) {
		throw new AssertionFailedError(message);

	}

}
