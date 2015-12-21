'use strict';

const crypto = require('crypto');
const https = require('https');
const util = require('util');

const async = require('async');
const aws4 = require('aws4');
const awscred = require('awscred');
const lruCache = require('lru-cache');
const once = require('once');
const stream = require('stream');

const listStreams = function (options, cb) {
    if (!cb) {
        cb = options;
        options = {};
    }

    request('ListStreams', {}, options, (err, res) => {
        if (err) {
            return cb(err);
        }

        return cb(null, res.StreamNames);
    });
};

const defaultRetryPolicy = (makeRequest, options, cb) => {
    const initialRetryMs = options.initialRetryMs || 50;

    // Timeout doubles each time => ~51 sec timeout
    const maxRetries = options.maxRetries || 10;

    const errorCodes = options.errorCodes || [
        'EADDRINFO',
        'ETIMEDOUT',
        'ECONNRESET',
        'ESOCKETTIMEDOUT',
        'ENOTFOUND',
        'EMFILE',
    ];

    const errorNames = options.errorNames || [
        'ProvisionedThroughputExceededException',
        'ThrottlingException',
    ];

    const expiredNames = options.expiredNames || [
        'ExpiredTokenException',
        'ExpiredToken',
        'RequestExpired',
    ];

    const retry = i =>
        makeRequest((err, data) => {
            if (!err || i >= maxRetries) {
                return cb(err, data);
            }

            if (err.statusCode == 400 && ~expiredNames.indexOf(err.name)) {
                return awscred.loadCredentials(function(err, credentials) {
                    if (err) {
                        return cb(err);
                    }

                    options.credentials = credentials;

                    return makeRequest(cb);
                });
            }

            if (err.statusCode >= 500 || ~errorCodes.indexOf(err.code) || ~errorNames.indexOf(err.name)) {
                return setTimeout(retry, initialRetryMs << i, i + 1);
            }

            return cb(err);
        });

    return retry(0);
};

const resolveOptions = options => {
    const region = options.region;

    options = Object.keys(options).reduce((clone, key) => {
        clone[key] = options[key];

        return clone;
    }, {});

    if (typeof region === 'object' && region !== null) {
        options.host = options.host || region.host;
        options.port = options.port || region.port;
        options.region = options.region || region.region;
        options.version = options.version || region.version;
        options.agent = options.agent || region.agent;
        options.https = options.https || region.https;
        options.credentials = options.credentials || region.credentials;
    } else if (/^[a-z]{2}\-[a-z]+\-\d$/.test(region)) {
        options.region = region;
    } else if (!options.host) {
        // Backwards compatibility for when 1st param was host
        options.host = region;
    }
    if (!options.version) {
        options.version = '20131202';
    }

    return options;
};

const request = function (action, data, options, cb) {
    if (!cb) {
        cb = options;
        options = {};
    }

    if (!cb) {
        cb = data;
        data = {};
    }

    cb = once(cb);

    options = resolveOptions(options);

    const loadCreds = function (cb) {
        const needRegion = !options.region;
        const needCreds = !options.credentials || !options.credentials.accessKeyId || !options.credentials.secretAccessKey;

        if (needRegion && needCreds) {
            return awscred.load(cb);
        }

        if (needRegion) {
            return awscred.loadRegion((err, region) => {
                cb(err, {
                    region: region
                });
            });
        }

        if (needCreds) {
            return awscred.loadCredentials(function(err, credentials) {
                cb(err, {
                    credentials: credentials
                });
            });
        }

        cb(null, {});
    };

    loadCreds((err, creds) => {
        if (err) {
            return cb(err);
        }

        if (creds.region) {
            options.region = creds.region;
        }

        if (creds.credentials) {
            if (!options.credentials) {
                options.credentials = creds.credentials;
            } else {
                Object.keys(creds.credentials).forEach(key => {
                    if (!options.credentials[key]) {
                        options.credentials[key] = creds.credentials[key];
                    }
                });
            }
        }

        if (!options.region) {
            options.region = (options.host || '').split('.', 2)[1] || 'us-east-1';
        }

        if (!options.host) {
            options.host = 'kinesis.' + options.region + '.amazonaws.com';
        }

        const httpOptions = {};
        const body = JSON.stringify(data);
        const retryPolicy = options.retryPolicy || defaultRetryPolicy;

        httpOptions.host = options.host;
        httpOptions.port = options.port;

        httpOptions.method = 'POST';
        httpOptions.path = '/';
        httpOptions.body = body;

        if (options.agent !== null) {
            httpOptions.agent = options.agent;
        }

        if (options.timeout !== null) {
            httpOptions.timeout = options.timeout;
        }

        if (options.region !== null) {
            httpOptions.region = options.region;
        }

        // Don't worry about self-signed certs for localhost/testing
        if (httpOptions.host == 'localhost' || httpOptions.host == '127.0.0.1') {
            httpOptions.rejectUnauthorized = false;
        }

        httpOptions.headers = {
            'Host': httpOptions.host,
            'Content-Length': Buffer.byteLength(body),
            'Content-Type': 'application/x-amz-json-1.1',
            'X-Amz-Target': 'Kinesis_' + options.version + '.' + action,
        };

        const makeRequest = cb => {
            httpOptions.headers.Date = new Date().toUTCString();

            aws4.sign(httpOptions, options.credentials);

            const req = https.request(httpOptions, res => {
                let json = new Buffer(0);

                res.on('error', cb);

                res.on('data', chunk => {
                    json = Buffer.concat([json, chunk]);
                });

                res.on('end', function() {
                    let response, parseError;

                    if (json) {
                        try {
                            response = JSON.parse(json.toString('utf-8'));
                        } catch (e) {
                            parseError = e;
                        }
                    }

                    if (res.statusCode == 200 && !parseError) {
                        return cb(null, response);
                    }

                    var error = new Error();
                        error.statusCode = res.statusCode;

                    if (response !== null) {
                        error.name = (response.__type || '').split('#').pop();
                        error.message = response.message || response.Message || JSON.stringify(response);
                    } else {
                        if (res.statusCode == 413) {
                            json = 'Request Entity Too Large';
                        }

                        error.message = 'HTTP/1.1 ' + res.statusCode + ' ' + json;
                    }

                    cb(error);
                });
            });

            req.on('error', cb);

            if (options.timeout !== null) {
                req.setTimeout(options.timeout);
            }

            req.end(body);

            return req;
        };

        return retryPolicy(makeRequest, options, cb);
    });
};

const bignumCompare = (a, b) => {
    if (!a) {
        return -1;
    }

    if (!b) {
        return 1;
    }

    const lengthDiff = a.length - b.length;

    if (lengthDiff !== 0) {
        return lengthDiff;
    }

    return a.localeCompare(b);
};

class KinesisStream extends stream.Duplex {
    constructor (options) {
        if (typeof options == 'string') {
            options = {
                name: options
            };
        }

        if (!options || !options.name) {
            throw new Error('A stream name must be given');
        }

        super({
            objectMode: true
        });

        this.options = options;
        this.name = options.name;

        this.writeConcurrency = options.writeConcurrency || 1;
        this.sequenceCache = lruCache(options.cacheSize || 1000);

        this.currentWrites = 0;
        this.fetching = false;
        this.paused = true;

        this.shards = [];
        this.buffer = [];
    }

    _read () {
        this.paused = false;
        this.drainBuffer();
    }

    drainBuffer () {
        if (this.paused) {
            return;
        }

        while (this.buffer.length) {
            // TODO: refactor
            if (!this.push(this.buffer.shift())) {
                this.paused = true;

                return;
            }
        }

        if (this.fetching) {
            return;
        }

        this.fetching = true;

        this.getNextRecords(err => {
            this.fetching = false;

            if (err) {
                this.emit('error', err);
            }

            // If all shards have been closed, the stream should end
            if (this.shards.every(shard => shard.ended)) {
                return this.push(null);
            }

            this.drainBuffer();
        });
    }

    getNextRecords (cb) {
        this.resolveShards((err, shards) => {
            if (err) {
                return cb(err);
            }

            async.each(shards, () => {
                this.getShardIteratorRecords();
            }, cb);
        });
    }

    resolveShards (cb) {
        if (this.shards.length) {
            return cb(null, this.shards);
        }

        const resolveShards = (err, shards) => {
            if (err) {
                return cb(err);
            }

            this.shards = shards.map(shard => {
                if (typeof shard == 'string') {
                    return {
                        id: shard,
                        readSequenceNumber: null,
                        writeSequenceNumber: null,
                        nextShardIterator: null,
                        ended: false,
                    };
                } else {
                    return shard;
                }
            });

            cb(null, this.shards);
        };

        if (this.options.shards) {
            resolveShards(null, this.options.shards);
        } else {
            this.getShardIds(resolveShards);
        }
    }

    getShardIds (cb) {
        const parseShards = (err, res) => {
            if (err) {
                return cb(err);
            }

            const shards = res.StreamDescription.Shards
                            .filter(shard => !(shard.SequenceNumberRange && shard.SequenceNumberRange.EndingSequenceNumber))
                            .map(shard => shard.ShardId);

            cb(null, shards);
        };

        request('DescribeStream', {
            StreamName: this.name
        }, this.options, parseShards);
    }

    getShardIteratorRecords (shard, cb) {
        const data = {
            StreamName: this.name,
            ShardId: shard.id
        };

        let getShardIterator;

        if (shard.nextShardIterator === null) {
            if (shard.readSequenceNumber === null) {
                if (this.options.oldest) {
                    data.ShardIteratorType = 'TRIM_HORIZON';
                } else {
                    data.ShardIteratorType = 'LATEST';
                }
            } else {
                data.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
                data.StartingSequenceNumber = shard.readSequenceNumber;
            }

            getShardIterator = cb => {
                request('GetShardIterator', data, this.options, (err, res) => {
                    if (err) {
                        return cb(err);
                    }

                    cb(null, res.ShardIterator);
                });
            };
        } else {
            getShardIterator = cb => {
                cb(null, shard.nextShardIterator);
            };
        }

        getShardIterator((err, shardIterator) => {
            if (err) {
                return cb(err);
            }

            this.getRecords(shard, shardIterator, function(err, records) {
                if (err) {
                    // Try again if the shard iterator has expired
                    if (err.name == 'ExpiredIteratorException') {
                        shard.nextShardIterator = null;

                        return this.getShardIteratorRecords(shard, cb);
                    }

                    return cb(err);
                }

                if (records.length) {
                    shard.readSequenceNumber = records[records.length - 1].SequenceNumber;
                    this.buffer = this.buffer.concat(records);
                    this.drainBuffer();
                }

                cb();
            });
        });
    }

    getRecords (shard, shardIterator, cb) {
        const data = {
            StreamName: this.name,
            ShardId: shard.id,
            ShardIterator: shardIterator
        };

        request('GetRecords', data, this.options, (err, res) => {
            if (err) {
                return cb(err);
            }

            // If the shard has been closed the requested iterator will not return any more data
            if (res.NextShardIterator === null) {
                shard.ended = true;

                return cb(null, []);
            }

            shard.nextShardIterator = res.NextShardIterator;

            res.Records.forEach(function(record) {
                record.ShardId = shard.id;

                record.Data = new Buffer(record.Data, 'base64');
            });

            return cb(null, res.Records);
        });
    }

    _write (data, encoding, cb) {
        if (Buffer.isBuffer(data)) {
            data = {
                Data: data
            };
        }

        if (Buffer.isBuffer(data.Data)) {
            data.Data = data.Data.toString('base64');
        }

        if (!data.StreamName) {
            data.StreamName = this.name;
        }

        if (!data.PartitionKey) {
            data.PartitionKey = crypto.randomBytes(16).toString('hex');
        }

        let sequenceNumber;

        if (!data.SequenceNumberForOrdering) {
            // If we only have 1 shard then we can just use its sequence number
            if (this.shards.length == 1 && this.shards[0].writeSequenceNumber) {
                data.SequenceNumberForOrdering = this.shards[0].writeSequenceNumber;

                // Otherwise, if we have a shard ID already assigned, then use that
            } else if (data.ShardId) {
                for (let i = 0; i < this.shards.length; i++) {
                    if (this.shards[i].id == data.ShardId) {
                        if (this.shards[i].writeSequenceNumber) {
                            data.SequenceNumberForOrdering = this.shards[i].writeSequenceNumber;
                        }

                        break;
                    }
                }
                // Not actually supposed to be part of PutRecord
                delete data.ShardId;

                // Otherwise check if we have it cached for this PartitionKey
            } else if ((sequenceNumber = this.sequenceCache.get(data.PartitionKey)) !== null) {
                data.SequenceNumberForOrdering = sequenceNumber;
            }
        }

        this.currentWrites++;

        request('PutRecord', data, this.options, (err, responseData) => {
            this.currentWrites--;

            if (err) {
                this.emit('putRecord');
                return this.emit('error', err);
            }

            sequenceNumber = responseData.SequenceNumber;

            if (bignumCompare(sequenceNumber, this.sequenceCache.get(data.PartitionKey)) > 0) {
                this.sequenceCache.set(data.PartitionKey, sequenceNumber);
            }

            this.resolveShards((err, shards) => {
                if (err) {
                    this.emit('putRecord');

                    return this.emit('error', err);
                }

                for (let i = 0; i < shards.length; i++) {
                    if (shards[i].id != responseData.ShardId) {
                        continue;
                    }

                    if (bignumCompare(sequenceNumber, shards[i].writeSequenceNumber) > 0) {
                        shards[i].writeSequenceNumber = sequenceNumber;
                    }

                    this.emit('putRecord');
                }
            });
        });

        if (this.currentWrites < this.writeConcurrency) {
            return cb();
        }

        let onPutRecord = () => {
            this.removeListener('putRecord', onPutRecord);

            cb();
        };

        this.on('putRecord', onPutRecord);
    }
}

exports.stream = options => new KinesisStream(options);

exports.KinesisStream = KinesisStream;
exports.listStreams = listStreams;
exports.request = request;