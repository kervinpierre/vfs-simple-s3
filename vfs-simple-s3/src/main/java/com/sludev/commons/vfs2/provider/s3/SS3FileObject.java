/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sludev.commons.vfs2.provider.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.URLFileName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 * 
 * @author Kervin Pierre
 */
public class SS3FileObject extends AbstractFileObject
{
    private static final Logger log = LoggerFactory.getLogger(SS3FileObject.class);
    
    private final SS3FileSystem fileSystem;
    private S3Object currBlob;
    private ObjectMetadata currBlobProperties;
    
    /**
     * Creates a new FileObject for use with a remote S3 file or folder.
     * 
     * @param name
     * @param fileSystem 
     */
    protected SS3FileObject(final AbstractFileName name, final SS3FileSystem fileSystem)
    {
        super(name, fileSystem);
        this.fileSystem = fileSystem;
    }

    /**
     * Convenience method that returns the container ( i.e. "bucket ) and path from the current URL.
     * 
     * @return 
     */
    private Pair<String, String> getContainerAndPath()
    {
        Pair<String, String> res = null;
        
        try
        {
           URLFileName currName = (URLFileName)getName();
           
            String currPathStr = currName.getPath();
            currPathStr = StringUtils.stripStart(currPathStr, "/");
            
            if( StringUtils.isBlank(currPathStr) )
            {
                log.warn( 
                        String.format("getContainerAndPath() : Path '%s' does not appear to be valid", currPathStr));
                
                return null;
            }
            
            // Deal with the special case of the container root.
            if( StringUtils.contains(currPathStr, "/") == false )
            {
                // Container and root
                return new ImmutablePair<>(currPathStr, "/");
            }
            
            String[] resArray = StringUtils.split(currPathStr, "/", 2);
            
            res = new ImmutablePair<>(resArray[0], resArray[1]);
        }
        catch (Exception ex)
        {
            log.error( 
                  String.format("getContainerAndPath() : Path does not appear to be valid"), ex);
        }
        
        return res;
    }
    
    /**
     * Callback used when this FileObject is first used.  We connect to the remote
     * server and check early so we can 'fail-fast'.  If there are no issues then
     * this FileObject can be used.
     * 
     * @throws Exception 
     */
    @Override
    protected void doAttach() throws Exception
    {
        Pair<String, String> path = getContainerAndPath();
        
        try
        {
            // Check the container.  Force a network call so we can fail-fast
            //boolean res = fileSystem.getClient().doesBucketExist(path.getLeft()); 
            if( objectExists(path.getLeft(), path.getRight()) )
            {
                currBlob = fileSystem.getClient().getObject(path.getLeft(), path.getRight());
            }
            else
            {
                currBlob = null;
            }
        }
        catch (RuntimeException ex)
        {
            log.error( String.format("doAttach() Exception for '%s' : '%s'", 
                                     path.getLeft(), path.getRight()), ex);
            
            throw ex;
        }
    }
    
    private boolean objectExists( String cont, String path )
    {
        boolean res = false;
        
        try 
        {
            S3Object object = fileSystem.getClient().getObject(cont, path);
            res = true;
        } 
        catch (AmazonServiceException ex) 
        {
            String errorCode = ex.getErrorCode();
            if (!errorCode.equals("NoSuchKey")) 
            {
                throw ex;
            }
        }
        
        return res;
    }
    
    /**
     * Callback for checking the type of the current FileObject.  Typically can
     * be of type...
     * FILE for regular remote files
     * FOLDER for regular remote containers
     * IMAGINARY for a path that does not exist remotely.
     * 
     * @return
     * @throws Exception 
     */
    @Override
    protected FileType doGetType() throws Exception
    {
        FileType res;

        Pair<String, String> path = getContainerAndPath();

        if( objectExists(path.getLeft(), path.getRight()) )
        {
            res = FileType.FILE;
        }
        else
        {
            // Blob Service does not have folders.  Just files with path separators in
            // their names.
            
            // Here's the trick for folders.
            //
            // Do a listing on that prefix.  If it returns anything, after not
            // existing, then it's a folder.
            String prefix = path.getRight();
            if( prefix.endsWith("/") == false )
            {
                // We need folders ( prefixes ) to end with a slash
                prefix += "/";
            }

            ObjectListing blobs = null;
            if( prefix.equals("/") )
            {
                // Special root path case. List the root blobs with no prefix
                blobs = fileSystem.getClient().listObjects(path.getLeft());
            }
            else
            {
                blobs = fileSystem.getClient().listObjects(path.getLeft(), prefix);
            }
            
            if( blobs.getObjectSummaries().isEmpty() )
            {
                res = FileType.IMAGINARY;
            }
            else
            {
                res = FileType.FOLDER;
            }
        }
        
        return res;
    }

    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br />
     * @return a possible empty String array if the file is a directory or null or an exception if the
     * file is not a directory or can't be read.
     * @throws Exception if an error occurs.
     */
    @Override
    protected String[] doListChildren() throws Exception
    {
        String[] res = null;
        
        Pair<String, String> path = getContainerAndPath();

        String prefix = path.getRight();
        if( prefix.endsWith("/") == false )
        {
            // We need folders ( prefixes ) to end with a slash
            prefix += "/";
        }
        ListObjectsRequest loReq = new ListObjectsRequest();
        loReq.withBucketName(path.getLeft());
        loReq.withPrefix(prefix);
        loReq.withDelimiter("/");
        
        ObjectListing blobs = fileSystem.getClient().listObjects(loReq);
        
        List<String> resList = new ArrayList<>();
        for( S3ObjectSummary osum : blobs.getObjectSummaries() )
        {
            String currBlobStr = osum.getKey();
            resList.add( String.format("/%s/%s", path.getLeft(), currBlobStr) );
        }
        
        List<String> commPrefixes = blobs.getCommonPrefixes();
        if( commPrefixes != null )
        {
            for( String currFld : commPrefixes )
            {
                resList.add( String.format("/%s/%s", path.getLeft(), currFld) );
            }
        }
        
        res = resList.toArray(new String[resList.size()]);
        
        return res;
    }

    private void checkBlobProperties()
    {
        if( currBlobProperties == null )
        {
            currBlobProperties = currBlob.getObjectMetadata();
        }
    }
    
    /**
     * Upload a local file to Amazon S3.
     * 
     * @param f File object from the local file-system to be uploaded to Amazon S3
     */
    public void putObject(File f)
    {
        Pair<String, String> path = getContainerAndPath();
        
        fileSystem.getClient().putObject(
                new PutObjectRequest(path.getLeft(), path.getRight(), f) );
    }
    
    /**
     * Callback for handling "content size" requests by the provider.
     * 
     * @return The number of bytes in the File Object's content
     * @throws Exception 
     */
    @Override
    protected long doGetContentSize() throws Exception
    {
        long res = -1;
        
        checkBlobProperties();
        res = currBlobProperties.getContentLength();
        
        return res;
    }

    /**
     * Get an InputStream for reading the content of this File Object.
     * @return The InputStream object for reading.
     * @throws Exception 
     */
    @Override
    protected InputStream doGetInputStream() throws Exception
    {
        S3ObjectInputStream in = currBlob.getObjectContent();
        
        return in;
    }

    /**
     * Callback for handling delete on this File Object
     * @throws Exception 
     */
    @Override
    protected void doDelete() throws Exception
    {
        Pair<String, String> path = getContainerAndPath();
        
        // Purposely use the more restrictive delete() over deleteIfExists()
        fileSystem.getClient().deleteObject(path.getLeft(), path.getRight());
    }

    /**
     * Callback for handling create folder requests.  Since there are no folders
     * in Amazon S3 this call is ingored.
     * 
     * @throws Exception 
     */
    @Override
    protected void doCreateFolder() throws Exception
    {
        log.info(String.format("doCreateFolder() called."));
    }

    /**
     * Used for creating folders.  It's not used since S3 does not have
     * the concept of folders.
     * 
     * @throws FileSystemException 
     */
    @Override
    public void createFolder() throws FileSystemException
    {
        log.info(String.format("createFolder() called."));
    }

    /**
     * Callback for getting an OutputStream for writing into Amazon S3
     * @param bAppend  bAppend true if the file should be appended to, false if it should be overwritten.
     * @return An OutputStream for writing into Amazon S3
     * @throws Exception 
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception
    {
        OutputStream res = new SS3OutputStream(this);
        
        return res;
    }

    /**
     * Callback for use when detaching this File Object from Amazon S3.
     * 
     * The File Object should be reusable after <code>attach()</code> call.
     * @throws Exception 
     */
    @Override
    protected void doDetach() throws Exception
    {
        currBlob = null;
        currBlobProperties = null;
    }

    /**
     * Callback for handling the <code>getLastModifiedTime()</code> Commons VFS API call.
     * @return Time since the file has last been modified
     * @throws Exception 
     */
    @Override
    protected long doGetLastModifiedTime() throws Exception
    {
        long res;
        
        checkBlobProperties();
        Date lm = currBlobProperties.getLastModified();
        
        res = lm.getTime();
        
        return res;
    }
}
