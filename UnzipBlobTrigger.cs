using System;
using System.IO;
using System.Linq;
using System.IO.Compression;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;

namespace ah903.azure.data
{
    public class UnzipBlobTrigger
    {
        [FunctionName("UnzipBlobTrigger")]
        public void Run([BlobTrigger("landing/zips/{name}", Connection = "sadatalake02022022_STORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            string destinationStorageAccount = Environment.GetEnvironmentVariable("destinationStorage");
            string destinationContainerName = Environment.GetEnvironmentVariable("destinationContainer");
            string destinationStorageAccountName = destinationStorageAccount.Split(";")[1];
            log.LogInformation($"Target Storage:{destinationStorageAccountName}");
            log.LogInformation($"Container: {destinationContainerName}");

            try
            {
                if(name.Split(".").Last().ToLower()!="zip")
                {
                    log.LogInformation($"{name} Is not a Zip File");
                    //TODO: Process Non Zip Files
                    return;
                }
            
                ZipArchive archive = new ZipArchive(myBlob);
                
                //Get Blob Client for the Storage Account
                BlobServiceClient blobServiceClient = new BlobServiceClient(destinationStorageAccount);

                //Get Reference to the Container
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(destinationContainerName);

                log.LogInformation($"Processing Zip File {name}");

                foreach(ZipArchiveEntry entry in archive.Entries)
                {
                    log.LogInformation($"Extracting {entry.FullName} from {name}");
                    // Get the Blob Name to write to, prepend the Zip Name
                    BlobClient blobClient = containerClient.GetBlobClient(name + "/" + entry.FullName); 

                    using(var fileStream = entry.Open())
                    {
                        blobClient.UploadAsync(fileStream);
                    }
                }
            }
            catch(Exception ex){
                log.LogInformation(ex.Message);
            }       
        }
    }
}