var pkg = require('../package.json');
var amqp = require('amqplib/callback_api');
var Async = require('async');
var Hoek = require('hoek');
var internals = {
    done: 0
};

exports.register = function (server, userOptions, next) {
    //accept separated or as uri.
    var defaultOptions = {
        connection: {
            secure: false,
            host: 'localhost',
            port: 5672,
            user: 'guest',
            pass: 'guest',
            vhost: '%2F',
            uri: 'amqp://guest:guest@localhost:5672/%2F'
        }
    };

    Hoek.assert(userOptions.routes, 'options must routes array'); // no routes

    var options = Hoek.applyToDefaults(defaultOptions, userOptions);
    var routes = options.routes;
    var cfg = {};

    for (var i = 0; i < routes.length; i++) {
        cfg[i + ''] = internals.createRoute.bind({
            routeCfg: Hoek.clone(routes[i]),
            connCfg: Hoek.clone(options.connection)
        });
    }

    Async.auto(cfg, function (err, results) {
        if (err !== null) {
            throw err;
        }
        else {
            next();
        }
    });
};

exports.register.attributes = {
    pkg: require('../package.json')
};

//creates queues and exchanges with the required binding
internals.createRoute = function (callback) {
    Async.auto({
        config: internals.hydrateCfg.bind(this),
        testConn: ['config', function (done, results) {
            var uri = internals.getConnectionUri(results.config.connCfg);
            amqp.connect(uri, function (err, conn) {
                if (err !== null) {
                    throw err;
                } else {
                    done(null, conn)
                }
            });

        }],
        testConn2: ['config', function (done, results) {
            var uri = internals.getConnectionUri(results.config.connCfg);
            amqp.connect(uri, function (err, conn) {
                if (err !== null) {
                    throw err;
                } else {
                    done(null, conn)
                }
            });
        }],
        testChannel: ['testConn', function (done, results) {
            results.testConn.createChannel(function (err, channel) {
                if (err !== null) {
                    throw err;
                } else {
                    channel.on('error', function (error) {
                        if (error.code !== 404) {
                            throw error;
                        }
                    });
                    done(null, channel);
                }
            });

        }],
        testChannel2: ['testConn2', function (done, results) {
            results.testConn2.createChannel(function (err, channel) {
                if (err !== null) {
                    throw err;
                } else {
                    channel.on('error', function (error) {
                        if (error.code !== 404) {
                            throw error;
                        }
                    });
                    done(null, channel);
                }
            });
        }],
        testExchange: ['testChannel', function (done, results) {
            var clone = Hoek.clone(results.config.routeCfg.exchange);

            results.testChannel.checkExchange(clone.name, function (err, ok) {
                if (err !== null) {
                    done(null, false);
                }
                else {
                    done(null, true);
                }
            });
        }],
        testQueue: ['testChannel2', function (done, results) {
            var clone = Hoek.clone(results.config.routeCfg.queue);

            results.testChannel2.checkQueue(clone.name, function (err, ok) {
                if (err !== null) {
                    done(null, false);
                }
                else {
                    done(null, true);
                }
            });
        }],
        conn: ['testExchange', 'testQueue', function (done, results) {
            var uri = internals.getConnectionUri(results.config.connCfg);
            amqp.connect(uri, function (err, conn) {
                if (err !== null) {
                    throw err;
                } else {
                    done(null, conn)
                }
            });
        }],
        channel: ['conn', function (done, results) {
            results.conn.createChannel(function (err, channel) {
                if (err !== null) {
                    throw err;
                } else {
                    done(null, channel);
                }
            });

        }],
        exchange: ['channel', function (done, results) {
            var exchangeCfg = results.config.routeCfg.exchange;
            if (results.testExchange === false) {
                results.channel.assertExchange(exchangeCfg.name, exchangeCfg.type, exchangeCfg.options, function (err, ok) {
                    if (err !== null) {
                        done(err, false);
                    }
                    else {
                        done(null, true);
                    }
                });
            } else {
                done(null, true);
            }
        }],
        queue: ['channel', function (done, results) {
            var queueCfg = results.config.routeCfg.queue;
            if (results.testQueue === false) {
                results.channel.assertQueue(queueCfg.name, queueCfg.options, function (err, ok) {
                    if (err !== null) {
                        done(err, false);
                    }
                    else {
                        done(null, true);
                    }
                });
            } else {
                done(null, true);
            }
        }],
        bind: ['exchange', 'queue', function (done, results) {
            var exchangeCfg = results.config.routeCfg.exchange;
            var queueCfg = results.config.routeCfg.queue;
            var routingKey = queueCfg.routingKey || '';
            var bindingArgs = queueCfg.bindingArgs || {};
            results.channel.bindQueue(queueCfg.name, exchangeCfg.name, routingKey, bindingArgs, function (err, ok) {
                if (err !== null) {
                    done(err, false);
                }
                else {
                    done(null, true);
                }
            });
        }],
        consumer: ['bind', function (done, results) {
            var queueCfg = results.config.routeCfg.queue;
            //channel needs to be forwarded so the ack is done
            results.channel.consume(queueCfg.name, function (msg) {
                results.config.routeCfg.handler(msg, results.channel);
            }, {}, function (err, ok) {
                if (err !== null) {
                    done(err, false);
                }
                else {
                    done(null, true);
                }
            });
        }]
    }, function (error, results) {
        callback(error, results);
    });
};

//forward configuration to callback
internals.hydrateCfg = function (callback) {
    callback(null, this);
};

//creates the connection uri given the connection object option
internals.getConnectionUri = function (conn) {
    if (conn.uri) {
        return conn.uri;
    }

    var uri = (conn.secure === true) ? 'amqps://' : 'amqp://';
    var vhost = conn.vhost.replace('/', '%2F');

    uri = uri + conn.user + ':' + conn.pass + '@' + conn.host + ':' + conn.port + '/' + vhost;

    return uri;
};