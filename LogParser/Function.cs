using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using LogParser.Model;
using Newtonsoft.Json;

namespace LogParser {
    
    //--- Classes ---
    public class Function {
    
        //--- Fields ---
        private readonly string logsBucket = Environment.GetEnvironmentVariable("LOGS_BUCKET");
        private readonly IAmazonS3 _s3Client;
        
        //--- Constructors ---
        public Function() {
            _s3Client = new AmazonS3Client();
        }

        //--- Methods ---
        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
        public void Handler(CloudWatchLogsEvent cloudWatchLogsEvent, ILambdaContext context) {
            
            // Level 1: decode and decompress data
            var logsData = cloudWatchLogsEvent.AwsLogs.Data;
            Console.WriteLine($"THIS IS THE DATA: {logsData}");
            var decompressedData = DecompressLogData(logsData);
            Console.WriteLine($"THIS IS THE DECODED, UNCOMPRESSED DATA: {decompressedData}");

            // Level 2: Parse log records
            var athenaFriendlyJson = ParseLog(decompressedData);

            // Level 3: Save data to S3
            PutObject(athenaFriendlyJson);

            // Level 4: Create athena schema to query data
        }
        
        public static string DecompressLogData(string value) {
            var b = Convert.FromBase64String(value);
            using (var msi = new MemoryStream(b))
            using (var mso = new MemoryStream()) {
                using (var gs = new GZipStream(msi, CompressionMode.Decompress)) {
                    //gs.CopyTo(mso);
                    gs.CopyTo(mso);
                }

                return Encoding.UTF8.GetString(mso.ToArray());
            }
        }

        private static List<TwitterData> ParseLog(string data) {
            var myObject = JsonConvert.DeserializeObject<DecompressedEvents>(data);
            var twitterDataArr = myObject.LogEvents.AsParallel().WithDegreeOfParallelism(8).Select(x => {
                var messageArr = x.Message.Split(new[] { "λ" }, StringSplitOptions.None);
                var matchPattern = @"(\(\w+\))";
                var userList = Regex.Matches(messageArr[0], matchPattern).Cast<Match>().Select(str => str.Value).ToList();
                var messageList = Regex.Matches(messageArr[1], matchPattern).Cast<Match>().Select(str => str.Value).ToList();
                var tweetList = Regex.Matches(messageArr[2], matchPattern).Cast<Match>().Select(str => str.Value).ToList();
                var userInfo = new UserInfo{
                    UserName = userList[0],
                    DisplayName = userList[1],
                    FavoriteTweets = userList[2],
                    TweetedTimes = userList[3],
                    Friends = userList[4],
                    Following = userList[5],
                    Created = userList[6]
                };
                var messageInfo = new MessageInfo {
                    UserName = messageList[0],
                    Message = messageList[1]
                };
                var tweetInfo = new TweetInfo {
                    Retweet = tweetList[0],
                    Favorited = tweetList[1]
                };
                return new TwitterData {
                    User = userInfo,
                    Message = messageInfo,
                    Tweet = tweetInfo
                };
            }).ToList();
            return twitterDataArr;
            
        }

        public void PutObject(List<TwitterData> values) {
            var request = new PutObjectRequest();
            request.ContentBody = JsonConvert.SerializeObject(values);
            request.BucketName = logsBucket;
            request.Key = $"NewContent-{DateTime.Now}";
            request.ContentType = "application/json";
            request.CannedACL = S3CannedACL.PublicRead;
            _s3Client.PutObjectAsync(request);
        }
    }
}
