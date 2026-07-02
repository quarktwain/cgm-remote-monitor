'use strict';
require('should');

// Companion to notifications.multi-instance.test.js, covering the WARM-instance
// half of issue #8194 and the fail-open guarantees of the storage re-read.
//
// #8194 background: alarm snooze/ack state is externalized to Mongo (PR #8501) so
// multiple instances share it. But getSnooze() is read only when an alarm object
// is first created, and the in-process `alarms` map is never cleared in production
// (the only reset is resetStateForTests). So a long-lived "warm" instance that
// built its alarm object before a cross-instance ack must RE-READ storage before
// emitting, or it keeps firing from stale lastAckTime/silenceTime.
//
// Because that re-read sits on a medical-alarm hot path, it must be fail-open: a
// storage error or a slow read must never DROP an alarm -- worst case it fires an
// extra time, never zero times.

// ---- shared harness (mirrors notifications.multi-instance.test.js) ----

function makeSharedStorage () {
  var stored = {};
  return {
    _stored: stored,
    getSnooze: async function (level, group) {
      var key = level + '-' + (group || 'default');
      var doc = stored[key];
      if (doc && doc.expiresAt > new Date()) return doc;
      return null;
    },
    setSnooze: async function (level, group, lastAckTime, silenceTime) {
      var ng = group || 'default';
      var key = level + '-' + ng;
      var newExpiresAt = new Date(lastAckTime + silenceTime);
      var existing = stored[key];
      if (!existing || newExpiresAt > existing.expiresAt) {
        stored[key] = {
          _id: key, level: level, group: ng,
          lastAckTime: lastAckTime, silenceTime: silenceTime,
          expiresAt: newExpiresAt
        };
      }
      return null;
    }
  };
}

function makeInstance (storage) {
  delete require.cache[require.resolve('../lib/notifications')];
  var emits = [];
  var ctx = {
    ddata: { lastUpdated: Date.now() },
    bus: { emit: function (evt, data) { emits.push({ evt: evt, data: data }); } },
    levels: { URGENT: 2, WARN: 1, INFO: 0, toDisplay: function (l) { return 'L' + l; } },
    alarmStorage: storage
  };
  var n = require('../lib/notifications')({ testMode: true }, ctx);
  n.resetStateForTests();
  n.__ctx = ctx;
  n.__emits = emits;
  return n;
}

// Simulate a fresh CGM reading arriving at an instance and being processed.
function feedReading (instance) {
  instance.__ctx.ddata.lastUpdated = Date.now();
  instance.initRequests();
  instance.requestNotify({
    level: 2, group: 'default',
    title: 'High', message: 'High BG',
    plugin: { name: 'test' }
  });
  instance.process();
}

function emittedNotify (instance) {
  return instance.__emits.some(function (e) {
    return e.evt === 'notification' && !e.data.clear;
  });
}

// Let the async getSnooze()/resolveRefresh chain settle (two microtask ticks).
function settle () { return new Promise(function (r) { setImmediate(function () { setImmediate(r); }); }); }
function delay (ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

describe('notifications multi-instance (WARM instance) -- #8194', function () {

  var STALE_MS = 5;          // shrink the refresh-staleness window for the test
  var CROSS_DELAY_MS = 50;   // comfortably exceeds STALE_MS before B re-reads
  var savedStaleEnv;

  before(function () {
    savedStaleEnv = process.env.ALARM_REFRESH_STALE_MS;
    process.env.ALARM_REFRESH_STALE_MS = String(STALE_MS);
  });

  after(function () {
    if (savedStaleEnv === undefined) { delete process.env.ALARM_REFRESH_STALE_MS; }
    else { process.env.ALARM_REFRESH_STALE_MS = savedStaleEnv; }
  });

  it('a warm instance honors a later ack made on another instance', async function () {
    var sharedStorage = makeSharedStorage();
    var instanceA = makeInstance(sharedStorage);
    var instanceB = makeInstance(sharedStorage);

    // 1) B sees the alarm FIRST and creates its (warm) alarm object while storage
    //    is empty, then emits once the refresh window resolves.
    feedReading(instanceB);
    await settle();
    emittedNotify(instanceB).should.equal(true,
      'sanity: a warm instance emits the first alarm when no snooze exists yet');

    // 2) The user acks on instance A. This persists the snooze to shared storage.
    instanceA.ack(2, 'default', 60 * 60 * 1000); // 60-minute snooze
    await settle();
    sharedStorage._stored['2-default'].should.be.an.Object(); // sanity: persisted

    // 3) After the staleness window elapses, a new CGM reading routes to the warm
    //    instance B. B must re-read storage and honor A's ack.
    instanceB.__emits.length = 0;
    await delay(CROSS_DELAY_MS);
    feedReading(instanceB);
    await settle();
    emittedNotify(instanceB).should.equal(false,
      'Warm instance B re-emitted despite the ack on instance A ' +
      '-- #8194 regression for long-lived multi-instance deployments');
  });
});

describe('notifications storage re-read is fail-open -- #8194', function () {

  // A short refresh deadline keeps the "hang past deadline" case fast.
  var DEADLINE_MS = 30;
  var savedTimeoutEnv;

  before(function () {
    savedTimeoutEnv = process.env.ALARM_REFRESH_TIMEOUT_MS;
    process.env.ALARM_REFRESH_TIMEOUT_MS = String(DEADLINE_MS);
  });

  after(function () {
    if (savedTimeoutEnv === undefined) { delete process.env.ALARM_REFRESH_TIMEOUT_MS; }
    else { process.env.ALARM_REFRESH_TIMEOUT_MS = savedTimeoutEnv; }
  });

  it('emits the alarm when getSnooze() rejects (storage error must not drop it)', async function () {
    var rejectingStorage = {
      getSnooze: async function () { throw new Error('mongo unavailable'); },
      setSnooze: async function () { return null; }
    };
    var inst = makeInstance(rejectingStorage);

    feedReading(inst);
    await settle();
    emittedNotify(inst).should.equal(true,
      'a getSnooze() rejection must fall through to emit, not silently drop the alarm');
  });

  it('emits from in-memory state when getSnooze() hangs past the deadline', async function () {
    var hangingStorage = {
      getSnooze: function () { return new Promise(function () { /* never resolves */ }); },
      setSnooze: async function () { return null; }
    };
    var inst = makeInstance(hangingStorage);

    // First reading is deferred while the (hung) read is in flight.
    feedReading(inst);
    await settle();
    emittedNotify(inst).should.equal(false, 'sanity: deferred while within the deadline window');

    // After the deadline elapses, the next reading emits from in-memory state
    // rather than waiting forever on the stuck read.
    inst.__emits.length = 0;
    await delay(DEADLINE_MS * 2);
    feedReading(inst);
    await settle();
    emittedNotify(inst).should.equal(true,
      'a hung storage read must not block the alarm past the refresh deadline');
  });
});
