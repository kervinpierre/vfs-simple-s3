# vfs-simple-s3
A simple Apache Commons VFS Provider for Amazon Simple Storage Service ( S3 )

Azure provider for Apache Commons VFS - http://commons.apache.org/proper/commons-vfs/

From the website...
"Commons VFS provides a single API for accessing various different file systems. It presents a uniform view of the files from various different sources, such as the files on local disk, on an HTTP server, or inside a Zip archive."

Other Commons VFS Amazon S3 providers exists, and may contain more features.  But this project aims to...

* Keep lean by keeping the 'None-Apache' dependencies to a minimum.  
  * I.e. No Spring dependency, etc.
* Use standard libraries unaltered
  * No requiring patched versions of core libraries
* Use of standard Maven repository
  * Eventually after further testing...
  
### Example 1
```java
// Get some account credential information from properties or anywhere you need
String currAccountStr = testProperties.getProperty("s3.access.id"); 
String currKey = testProperties.getProperty("s3.access.secret");
String currContainerStr = testProperties.getProperty("s3.test0001.bucket.name");

// This can be "s3.amazonaws.com" for standard region S3
String currHost = testProperties.getProperty("s3.host");
String currRegion = testProperties.getProperty("s3.region");
String currFileNameStr;

// Create a temp file for uploading.  For testing purposes
File temp = File.createTempFile("uploadFile01", ".tmp");
try(FileWriter fw = new FileWriter(temp))
{
    BufferedWriter bw = new BufferedWriter(fw);
    bw.append("testing...");
    bw.flush();
}

// Creates a new S3 File Provider for our use
SS3FileProvider currSS3 = new SS3FileProvider();

// Optional set endpoint.  Not needed for standard zone
//currSS3.setEndpoint(currHost);

// Optional set region.  Not needed for stardard region
//currSS3.setRegion(currRegion);

// Create a default Commons VFS File-System Manager.  Add the 2 providers we
// plan on using
DefaultFileSystemManager currMan = new DefaultFileSystemManager();
currMan.addProvider(SS3Constants.S3SCHEME, currSS3);
currMan.addProvider("file", new DefaultLocalFileProvider());

// Initial the FS Manager before use
currMan.init(); 

// Set our authentication for S3.  Setting Account Name and Account Secret.
StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
FileSystemOptions opts = new FileSystemOptions(); 
DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 

// Create the remote Amazon S3 URL for the file we're about to upload...
currFileNameStr = "test01.tmp";
// Note that the URL scheme also has the "host" or "authority" set.
// E.g. s3://s3.amazonaws.com/<containter>/<path>
// This is not typical for many "s3://" schemed URLs but is required with this library.
String currUriStr = String.format("%s://%s/%s/%s", 
                   SS3Constants.S3SCHEME, currHost, currContainerStr, currFileNameStr);
FileObject currFile = currMan.resolveFile(currUriStr, opts);

// Do the same for the local temp file we're about to upload to Amazon S3
FileObject currFile2 = currMan.resolveFile(
        String.format("file://%s", temp.getAbsolutePath()));

// Run the actual upload
currFile.copyFrom(currFile2, Selectors.SELECT_SELF);

// Delete the test/temp file since we're done with it
temp.delete();
```
        
