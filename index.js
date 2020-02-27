var AWS = require("aws-sdk")
var s3 = new AWS.S3()

const pThrottle = require('p-throttle');
const throttledSync = pThrottle(syncObjects, 1000, 1000);

function syncObjects(from, to, params) {
  return s3.upload(Object.assign(params || {}, {
    Bucket: to.bucket,
    Key: to.key,
    Body: s3.getObject({ Bucket: from.bucket, Key: from.key }).createReadStream()
  })).promise()
}

function fetchList(bucket, prefix, ContinuationToken) {
  var params = {
    Bucket: bucket,
    MaxKeys: 1000,
    Prefix: prefix,
    ContinuationToken
  };
  return s3.listObjectsV2(params).promise();
}

async function listAll(bucket, prefix) {
  var allFiles = []
  var fetchedList = {};
  do {
    fetchedList = await fetchList(bucket, prefix, fetchedList.NextContinuationToken);
    allFiles = allFiles.concat(fetchedList.Contents);
  } while (fetchedList.NextContinuationToken);

  return allFiles.map((c) => {
    c.Key = c.Key.replace(prefix, "")
    return c
  })
}

async function sync(fromBucket, fromFolder, toBucket, toFolder) {

  var fromList = await listAll(fromBucket, fromFolder)
  var toList = await listAll(toBucket, toFolder)

  var toEtags = []
  toList.forEach((file) => {
    toEtags.push(file.ETag)
  })

  var fromByEtag = {}
  fromList.forEach((file) => {
    if (toEtags.indexOf(file.ETag) == -1) {
      fromByEtag[file.ETag] = file
    }
  })

  var allSyncPromises = []
  var count = 0
  var bytes = 0
  for (tag in fromByEtag) {
    count++
    bytes += fromByEtag[tag].Size
    allSyncPromises.push(
      throttledSync({ bucket: fromBucket, key: fromFolder + fromByEtag[tag].Key }, { bucket: toBucket, key: toFolder + fromByEtag[tag].Key })
    )
  }

  await Promise.all(allSyncPromises)
  return { count, bytes }
}

module.exports = {
  sync
}

if (require.main == module) {
  (async () => {
    var originBucket = 's3Bucket'
    var fromFolder = 'destination-folder/'
    var destinationFolder = 's3Bucket' // (can be the same one)
    var toFolder = 'origin-folder/'

    await sync(originBucket, fromFolder, destinationFolder, toFolder)
    // { count: 53, bytes: 2449367 } 
  })()
}