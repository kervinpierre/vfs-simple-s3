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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main provider class in the Simple S3 Commons VFS provider.
 * 
 * This class can be declared and passed to the current File-system manager 
 * 
 * E.g....
 * <pre><code>
 * SS3FileProvider currSS3 = new SS3FileProvider();
 *
 * //Optional set endpoint
 * currSS3.setEndpoint(currHost);
 *
 * // Optional set region
 * currSS3.setRegion(currRegion);
 * 
 * DefaultFileSystemManager currMan = new DefaultFileSystemManager();
 * currMan.addProvider(SS3Constants.S3SCHEME, currSS3);
 * currMan.init(); 
 *
 * StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
 * FileSystemOptions opts = new FileSystemOptions(); 
 * DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
 * </code></pre>
 * 
 * @author Kervin Pierre
 */
public class SS3FileProvider
                  extends AbstractOriginatingFileProvider
{
    private static final Logger log = LoggerFactory.getLogger(SS3FileProvider.class);
    
    private static final FileSystemOptions DEFAULT_OPTIONS = new FileSystemOptions();
    
    public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[]
        {
            UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
        };
    
    private String endpoint;
    private Regions region;
    
    static final Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(new Capability[]
    {
        Capability.GET_TYPE,
        Capability.READ_CONTENT,
        Capability.APPEND_CONTENT,
        Capability.URI,
        Capability.ATTRIBUTES,
        Capability.RANDOM_ACCESS_READ,
        Capability.DIRECTORY_READ_CONTENT,
        Capability.LIST_CHILDREN,
        Capability.LAST_MODIFIED,
        Capability.GET_LAST_MODIFIED,
        Capability.CREATE,
        Capability.DELETE
    }));

    /**
     * Creates a new FileProvider object.
     */
    public SS3FileProvider()
    {
        super();
        setFileNameParser(SS3FileNameParser.getInstance());
        endpoint = null;
        region = null;
    }
    
    /**
     * In the case that we are not sent FileSystemOptions object, we need to have
     * one handy.
     * 
     * @return 
     */
    public FileSystemOptions getDefaultFileSystemOptions()
    {
        return DEFAULT_OPTIONS;
    }
    
    /**
     * Create FileSystem event hook
     * 
     * @param rootName
     * @param fileSystemOptions
     * @return
     * @throws FileSystemException 
     */
    @Override
    protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) throws FileSystemException
    {
        SS3FileSystem fileSystem = null;
        GenericFileName genRootName = (GenericFileName)rootName;
        
        AWSCredentials storageCreds;
        AmazonS3Client client;
        
        FileSystemOptions currFSO;
        UserAuthenticator ua;
        
        if( fileSystemOptions == null )
        {
            currFSO = getDefaultFileSystemOptions();
            ua = SS3FileSystemConfigBuilder.getInstance().getUserAuthenticator(currFSO);  
        }
        else
        {
            currFSO = fileSystemOptions;
            ua = DefaultFileSystemConfigBuilder.getInstance().getUserAuthenticator(currFSO);
        }
        
        UserAuthenticationData authData = null;
        try
        {
            authData = ua.requestAuthentication(AUTHENTICATOR_TYPES);
            
            String currAcct = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData,
                    UserAuthenticationData.USERNAME, UserAuthenticatorUtils.toChar(genRootName.getUserName())));
            
            String currKey =  UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData,
                    UserAuthenticationData.PASSWORD, UserAuthenticatorUtils.toChar(genRootName.getPassword())));
        
            storageCreds = new BasicAWSCredentials(currAcct, currKey);           
            
            client = new AmazonS3Client(storageCreds);
            
            if( StringUtils.isNoneBlank(endpoint) )
            {
                client.setEndpoint(endpoint);
            }
            
            if( region != null )
            {
                client.setRegion(region);
            }
            
            fileSystem = new SS3FileSystem(genRootName, client, fileSystemOptions);
        }
        finally
        {
            UserAuthenticatorUtils.cleanup(authData);
        }
        
        return fileSystem;
    }

    /**
     * Returns the provider's capabilities.
     * 
     * @return 
     */
    @Override
    public Collection<Capability> getCapabilities()
    {
        return capabilities;
    }
    
    /**
     * Set the S3 endpoint we should use.  This needs to be done before init() is called.
     * 
     * @param ep 
     */
    public void setEndpoint(String ep)
    {
        endpoint = ep;
    }

    /**
     * Returns the currently set region.
     * 
     * @return 
     */
    public Regions getRegion()
    {
        return region;
    }

    /**
     * Set the S3 Region we should use for this provider.
     * 
     * @param region 
     */
    public void setRegion(Regions region)
    {
        this.region = region;
    }
    
    /**
     * Sets the AWS Region but first converts from a String.
     * @param r 
     */
    public void setRegion(String r)
    {
        this.region = Regions.fromName(r);
    }
}
