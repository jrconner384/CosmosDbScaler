using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Http;

namespace JasonBytes.CosmosDbScaler
{
    public static class HttpTriggerCSharp
    {
        [FunctionName("HttpScaler")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var dbName = req.Headers["dbName"];
            var collectionName = req.Headers["collectionName"];
            var dbUrl = req.Headers["dbUrl"];
            var key = req.Headers["key"];
            var throughput = Convert.ToInt32(req.Headers["throughput"]);

            using (var client = new DocumentClient(new Uri(dbUrl), key, new ConnectionPolicy { UserAgentSuffix = " samples-net/3" }))
            {
                try
                {
                    client.CreateDatabaseIfNotExistsAsync(new Database { Id = dbName }).Wait();
                    var simpleCollection = CreateCollection(client).Result;
                    ChangeThroughput(client, simpleCollection).Wait();
                }
                catch (Exception e)
                {
                    log.LogError(new JObject(new JProperty("exception",
                        new JObject(
                            new JProperty("Message", e.Message),
                            new JProperty("StackTrace", e.StackTrace),
                            new JProperty("Data", e.Data)))).ToString());
                    return new ExceptionResult(e, true);
                }

                return new OkObjectResult(new JObject(new JProperty("Response",
                    new JObject(
                        new JProperty("Code", StatusCodes.Status200OK),
                        new JProperty("Message", $"Throughput modified to {throughput} for {dbName}/{collectionName}.")))));
            }

            async Task<DocumentCollection> CreateCollection(DocumentClient client)
            {
                return await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(dbName),
                    new DocumentCollection { Id = collectionName },
                    new RequestOptions { OfferThroughput = throughput });
            }

            async Task ChangeThroughput(DocumentClient client, DocumentCollection simpleCollection)
            {
                var offer = client.CreateOfferQuery().Where(o => o.ResourceLink == simpleCollection.SelfLink).AsEnumerable().Single();

                log.LogInformation(new JObject(new JProperty("Offer",
                    new JObject(
                        new JProperty("Type", offer.OfferType),
                        new JProperty("Version", offer.OfferVersion),
                        new JProperty("SelfLink", offer.SelfLink),
                        new JProperty("LastModified", offer.Timestamp)))).ToString());

                var newOffer = await client.ReplaceOfferAsync(new OfferV2(offer, throughput));
            }
        }
    }
}
