/**
 * Module dependencies
 */

var stack = require('poe-service-node');
var base64 = require('urlsafe-base64');
var createHash = require('crypto').createHash;

var db = {
  counts: {},
  get: function(key, cb) {
    cb(null, this.counts[key] || 0);
  },
  increment: function(keys, by, cb) {
    var counts = this.counts;
    keys.forEach(function(key) {
      counts[key] |= 0;
      counts[key] += by;
    });
    cb();
  }
};

/**
 * Expose the app
 */

module.exports = function(opts) {
  opts = opts || {};
  var app = stack(opts);

  var mod = opts.module || 'stats';

  var granularities = {
    hour: 3600,
    day: 3600 * 24,
    week: 3600 * 24 * 7,
    month: 3600 * 24 * 30 // TODO figure out the months
  };

  var g = {
    h: granularities.hour,
    d: granularities.day,
    w: granularities.w,
    m: granularities.m
  };

  /**
   * Get the total number of events for a given metric
   *
   * @param {String} metric
   * @param {String|Number} objID
   * @return {Number}
   */

  app.register(mod, 'get_total', function(req, res, next) {
    var metric = req.params[0];
    var objID = req.params[1];

    db.get(sha(metric, objID), function(err, total) {
      if (err) return next(err);
      res.send(total);
    });
  });

  /**
   * Get a list of keys in a range
   *
   * @param {String} metric
   * @param {String|Number} objID
   * @param {Number} begin
   * @param {Number} end
   * @param {String?} granularity
   * @return [{time: Number, id: String}]
   */

  app.register(mod, 'get_range', function(req, res, next) {
    var metric = req.params[0];
    var objID = req.params[1];
    var granType = req.params[4] || 'day';
    var granularity = granularities[granType];
    if (!granularity) return next(new Error('Invalid granularity: ' + granType));

    var begin = floorTime(req.params[2], granularity);
    var end = floorTime(req.params[3], granularity);

    var intervals = Math.floor((end - begin) / granularity) + 1;

    var arr = new Array(intervals);
    for (var i = 0; i < intervals; i++) {
      var date = begin + i * granularity;
      arr[i] = {
        date: date,
        id: encodeKey(metric, objID, granType, date)
      };
    }

    res.send(arr);
  });

  /**
   * Get the counter value for a given key
   *
   * @param {String} key
   * @return [count, isFrozen]
   */

  app.register(mod, 'get_count', function(req, res, next) {
    var key = req.params[0];
    db.get(key, function(err, count) {
      if (err) return next(err);

      var parts = decodeKey(key);

      var granularity = g[parts[0]];
      var time = parseInt(parts[1], 10);

      var isFrozen = time !== now(granularity);
      res.send([count, isFrozen]);
    });
  });

  /**
   * stats:increment_counter
   *
   * @param {String} metric
   * @param {String|Number} objID
   * @param {Number} time
   * @param {Number?} by
   * @return {Number}
   */

  app.register(mod, 'increment_count', function(req, res, next) {
    var metric = req.params[0];
    var objID = req.params[1];
    var time = req.params[2];
    var by = req.params[3] || 1;

    var keys = [
      sha(metric, objID)
    ];

    for (var k in g) {
      var t = floorTime(time, g[k]);
      keys.push(encodeKey(metric, objID, k, t));
    }

    db.increment(keys, by, function(err) {
      if (err) return next(err);
      res.send(0);
    });
  });

  return app;
};

/**
 * Floor the given time for the resolution
 */

function floorTime(time, res) {
  return Math.floor(time / res) * res;
}

/**
 * Give now time in the resolution
 */

function now(res) {
  return floorTime(Date.now()/1000, res);
}

/**
 * Encode a metric key
 */

function encodeKey(metric, id, granularity, time) {
  return sha(metric, id) + granularity.slice(0, 1) + time;
}

/**
 * Decode a metric key
 */

function decodeKey(key) {
  var info = key.slice(20);
  return [
    info.charAt(0),
    info.slice(1)
  ];
}

/**
 * Create a sha hash
 */

function sha() {
  return base64.encode(createHash('sha1')
                       .update(Array.prototype.join.call(arguments, '::'))
                       .digest()).slice(0, 20);
}
