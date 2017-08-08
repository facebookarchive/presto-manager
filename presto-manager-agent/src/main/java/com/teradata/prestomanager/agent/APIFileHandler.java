/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.teradata.prestomanager.agent;

import io.airlift.log.Logger;

import javax.ws.rs.core.Response;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status;

public class APIFileHandler
{
    private static final Logger LOGGER = Logger.get(APIFileHandler.class);
    private final Path baseDir;

    public APIFileHandler(Path baseDir)
    {
        this.baseDir = requireNonNull(baseDir, "Base directory is null");
    }

    public Response getFileNameList()
    {
        try {
            List<String> fileNames = AgentFileUtils.getFileNameList(baseDir);
            String namesToReturn = String.join("\r\n", fileNames);
            LOGGER.debug("Successfully retrieved the list of file names from directory '%s'", baseDir.toString());
            return Response.status(Status.OK).entity(namesToReturn).build();
        }
        catch (NotDirectoryException | NoSuchFileException e) {
            LOGGER.error(e, "Pre-configured directory '%s' is not found or not a directory", baseDir.toString());
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Directory not found or is not a directory").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to retrieve the list of file names from directory: %s", baseDir.toString());
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to retrieve the list of file names from directory").build();
        }
    }

    public Response getFile(String path)
    {
        try {
            String fileContent = AgentFileUtils.getFile(Paths.get(baseDir.toString(), path));
            LOGGER.debug("Successfully retrieved contents of file '%s'", path);
            return Response.status(Status.OK).entity(fileContent).build();
        }
        catch (FileNotFoundException e) {
            LOGGER.error(e, "File '%s' not found", path);
            return Response.status(Status.NOT_FOUND)
                    .entity("File not found").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to process file '%s'", path);
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Fail to process file").build();
        }
    }

    public Response replaceFileFromURL(String path, String url)
    {
        try {
            AgentFileUtils.replaceFile(Paths.get(baseDir.toString(), path), new URL(url));
            LOGGER.debug("Replaced file '%s' with file at url '%s'", path, url);
            return Response.status(Status.ACCEPTED)
                    .entity("File replaced").build();
        }
        catch (MalformedURLException | IllegalArgumentException e) {
            LOGGER.error(e, "Invalid URL: %s", url);
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid URL").build();
        }
        catch (FileNotFoundException e) {
            LOGGER.error(e, "File '%s' not found", path);
            return Response.status(Status.NOT_FOUND)
                    .entity("File not found").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to process file '%s'", path);
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to process file").build();
        }
    }

    public Response updateProperty(String path, String property, String value)
    {
        try {
            AgentFileUtils.updateProperty(Paths.get(baseDir.toString(), path), property, value);
            LOGGER.debug("Updated property '%s' in file '%s' to '%s'", property, path, value);
            return Response.status(Status.OK)
                    .entity("Successfully updated the property of file").build();
        }
        catch (FileNotFoundException e) {
            LOGGER.error(e, "File '%s' not found", path);
            return Response.status(Status.NOT_FOUND)
                    .entity("File not found").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to process file '%s'", path);
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to process file").build();
        }
    }

    public Response deleteFile(String path)
    {
        try {
            AgentFileUtils.deleteFile(Paths.get(baseDir.toString(), path));
            LOGGER.debug("File deleted", path);
            return Response.status(Status.ACCEPTED)
                    .entity("Deleting file in progress").build();
        }
        catch (FileNotFoundException | IllegalArgumentException e) {
            LOGGER.error(e, "File '%s' not found", path);
            return Response.status(Status.NOT_FOUND)
                    .entity("File not found").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to process file '%s'", path);
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to process file").build();
        }
    }

    public Response getFileProperty(String path, String property)
    {
        try {
            String value = AgentFileUtils.getFileProperty(Paths.get(baseDir.toString(), path), property);
            LOGGER.debug("Successfully retrieved property '%s' from file '%s'", property, path);
            return Response.status(Status.OK).entity(value).build();
        }
        catch (NoSuchElementException | FileNotFoundException e) {
            LOGGER.error(e, "File '%s' not found", path);
            return Response.status(Status.NOT_FOUND)
                    .entity("File not found").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to process file '%s'", path);
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to process file").build();
        }
    }

    public Response deletePropertyFromFile(String path, String property)
    {
        try {
            AgentFileUtils.removePropertyFromFile(Paths.get(baseDir.toString(), path), property);
            LOGGER.debug("Deleted property '%s' from file '%s'", property, path);
            return Response.status(Status.ACCEPTED)
                    .entity("Deleted property").build();
        }
        catch (NoSuchElementException | FileNotFoundException e) {
            LOGGER.error(e, "File '%s' not found", path);
            return Response.status(Status.NOT_FOUND)
                    .entity("File not found").build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to process file '%s'", path);
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to process file").build();
        }
    }
}
