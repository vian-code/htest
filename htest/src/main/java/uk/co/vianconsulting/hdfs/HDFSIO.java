package uk.co.vianconsulting.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

import uk.co.vianconsulting.hdfs.exception.HDFSIOException;

public class HDFSIO {

	private FileSystem fileSystem;

	public HDFSIO() throws HDFSIOException {
		try {
			fileSystem = readHDFSConfiguration();
		} catch (IOException e) {
			throw new HDFSIOException(e);
		}
	}

	public FileSystem readHDFSConfiguration() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));

		FileSystem fileSystem = FileSystem.get(conf);
		return fileSystem;
	}

	public Vector<Path> getAllFiles(Path path) throws IOException {

		FileStatus[] globStatus = fileSystem.globStatus(path);
		Vector<Path> files = new Vector<Path>();
		for (FileStatus status : globStatus) {
			files.add(status.getPath());
		}
		return files;
	}

	public void copyToLocal(String inPath, String outPath)
			throws HDFSIOException {

		try {
			fileSystem.copyToLocalFile(new Path(inPath), new Path(outPath));
		} catch (IOException e) {
			throw new HDFSIOException(e);
		}
	}

	public void copyFromLocal(String inPath, String outPath)
			throws HDFSIOException {

		try {
			fileSystem.copyFromLocalFile(new Path(inPath), new Path(outPath));
		} catch (IOException e) {
			throw new HDFSIOException(e);
		}
	}

	public void copyDirectory(Path inPath, Path outPath) throws HDFSIOException {
		try {
			Vector<Path> allFiles = getAllFiles(inPath);
			for (Path file : allFiles) {

				copyToHDFS(file,
						new Path(outPath.toString() + "/" + file.getName()));
			}
		} catch (IOException e) {
			throw new HDFSIOException(e);
		}
	}

	public synchronized void copyToHDFS(Path inPath, Path outPath)
			throws HDFSIOException {

		FSDataInputStream inStream = null;
		FSDataOutputStream outStream = null;
		try {
			if (!fileSystem.exists(inPath)) {
				throw new HDFSIOException("File " + inPath + " does not exist");

			}
			inStream = fileSystem.open(inPath);
			outStream = fileSystem.create(outPath);
			byte[] buf = new byte[10024];
			int data = 0;
			while ((data = inStream.read(buf)) > 0) {
				outStream.write(buf, 0, 10024);
			}
		} catch (IOException e) {
			throw new HDFSIOException(e);
		} finally {
			try {

				if (inStream != null) {
					inStream.close();
				}
				if (outStream != null) {
					outStream.close();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void deleteFile(String file) throws HDFSIOException {
		try {

			Path path = new Path(file);
			if (!fileSystem.exists(path)) {
				System.out.println("File " + file + " does not exists");
				return;
			}

			fileSystem.delete(new Path(file), true);

		} catch (IOException e) {
			throw new HDFSIOException(e);
		} finally {
		}
	}

	public void writeStringToHDFSFile(Path path, String text)
			throws HDFSIOException {
		FSDataOutputStream outStream = null;
		try {
			outStream = fileSystem.create(path);
			outStream.writeBytes(text);
		} catch (IOException e) {
			throw new HDFSIOException(e);
		} finally {
			try {

				if (outStream != null) {
					outStream.close();
				}
			} catch (IOException e) {
				throw new HDFSIOException(e);
			}
		}
	}

	public String readHDFSFileToString(Path path) throws HDFSIOException {
		FSDataInputStream inputStream = null;
		try {
			if (!fileSystem.exists(path)) {
				System.out.println("File " + path + " does not exists");
				return null;
			}

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
			throw new HDFSIOException(e);
		} finally {
			try {

				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				throw new HDFSIOException(e);
			}
		}
	}

	public boolean directoryUseable(String string) throws HDFSIOException {
		// TODO Auto-generated method stub
		Path path = new Path(string);
		try {
			if (!fileSystem.getFileStatus(path).isDir()) {
				throw new HDFSIOException("Path is not a directory: " + string);
			}
			if (!fileSystem.getFileStatus(path).getPermission().getUserAction()
					.implies(FsAction.WRITE)) {
				throw new HDFSIOException("Path is not writable: " + string);
			}
		} catch (IOException e) {
			throw new HDFSIOException(e);
		}
		return true;
	}

	public void close() throws HDFSIOException {
		try {
			fileSystem.close();
		} catch (IOException e) {
			throw new HDFSIOException(e);
		}
	}

	public static void main(String[] args) {
		try {
			HDFSIO io = new HDFSIO();
			io.directoryUseable("/poc3-iteration-4/unreadable-directory/");
			io.directoryUseable("/poc3-iteration-4/input/");
			// io.copyDirectory(new Path("/devl/*.csv"),new
			// Path("/devlcopytest"));
		} catch (HDFSIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
