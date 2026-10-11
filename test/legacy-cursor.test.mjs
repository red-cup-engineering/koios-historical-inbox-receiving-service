import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import {
  cloudEventId,
  loadCursor,
  stripSignatureFields,
} from "../../admit-set-of-inbox-arrivals.mjs";
import {admitSetOfInboxArrivals,convertSqlCursor} from "../receive.mjs";
import { KOIOS_ACTOR_URI } from "../../../src/server.mjs";

const TEST_DIR = dirname(fileURLToPath(import.meta.url));
const FIXTURE_DB_PATH = join(TEST_DIR, "../../../test/fixtures", "inbox-fixture.sqlite");

function createScratchDatabase() {
  const workDir = mkdtempSync(join(tmpdir(), "inbox-arrivals-"));
  const dbPath = join(workDir, "activitypub.sqlite");
  const database = new DatabaseSync(dbPath);
  database.exec(`
    CREATE TABLE activitypub_verified_arrivals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      content_sha256 TEXT NOT NULL UNIQUE,
      activity_json TEXT NOT NULL,
      receipt_json TEXT NOT NULL,
      recipient TEXT,
      observed_at INTEGER NOT NULL
    )
  `);
  const insert = database.prepare("INSERT INTO activitypub_verified_arrivals (content_sha256, activity_json, receipt_json, recipient, observed_at) VALUES (?, ?, ?, ?, ?)");
  return {
    workDir,
    dbPath,
    insertActivity(hash, activity, observedAt) {
      insert.run(hash, JSON.stringify(activity), "{}", null, observedAt);
    },
    close: () => database.close(),
  };
}

test("reads the fixture inbox, redacts signature/key fields, and sets source to the actor inbox URL", () => {
  const cursor = loadCursor("/nonexistent/cursor.json");
  const { events, cursor: nextCursor } = admitSetOfInboxArrivals({ dbPath: FIXTURE_DB_PATH, cursor });
  assert.equal(events.length, 2);
  assert.equal(events[0].source, `${KOIOS_ACTOR_URI}/inbox`);
  assert.equal(events[0].type, "org.561-group.activitypub-inbox-activity.v1");
  assert.equal(events[0].data.type, "Follow");
  assert.match(events[0].id, /^sha256:[0-9a-f]{64}$/u);

  const createEvent = events[1];
  assert.equal(createEvent.data.signature, undefined);
  assert.equal(createEvent.data.object.attributedTo.publicKeyPem, undefined);
  assert.equal(createEvent.data.object.content, "hello");
  assert.equal(nextCursor.lastId, 2);
});

test("second run against the unchanged fixture db is idempotent and emits nothing new", () => {
  const first = admitSetOfInboxArrivals({ dbPath: FIXTURE_DB_PATH, cursor: loadCursor("/nonexistent/cursor.json") });
  const second = admitSetOfInboxArrivals({ dbPath: FIXTURE_DB_PATH, cursor: first.cursor });
  assert.equal(first.events.length, 2);
  assert.deepEqual(second.events, []);
});

test("a row inserted after the cursor is admitted while prior rows are not re-admitted", () => {
  const scratch = createScratchDatabase();
  try {
    scratch.insertActivity("h1", { id: "https://example.test/a/1", type: "Follow" }, Date.parse("2026-09-01T00:00:00Z"));
    const first = admitSetOfInboxArrivals({ dbPath: scratch.dbPath, cursor: loadCursor("/nonexistent/cursor.json") });
    assert.equal(first.events.length, 1);

    scratch.insertActivity("h2", { id: "https://example.test/a/2", type: "Like" }, Date.parse("2026-09-01T01:00:00Z"));
    const second = admitSetOfInboxArrivals({ dbPath: scratch.dbPath, cursor: first.cursor });
    assert.equal(second.events.length, 1);
    assert.equal(second.events[0].data.type, "Like");
  } finally {
    scratch.close();
    rmSync(scratch.workDir, { recursive: true, force: true });
  }
});

test("admitSetOfInboxArrivals returns an empty array when the database file does not exist yet", () => {
  const { events, cursor } = admitSetOfInboxArrivals({ dbPath: "/nonexistent/activitypub.sqlite", cursor: loadCursor("/nonexistent/cursor.json") });
  assert.deepEqual(events, []);
  assert.equal(cursor.lastId, 0);
});

test("stripSignatureFields removes signature and publicKey fields at any depth, case-insensitively", () => {
  const stripped = stripSignatureFields({ id: "x", Signature: { r: 1 }, nested: { publicKeyPem: "y", keep: "z" } });
  assert.equal(stripped.id, "x");
  assert.equal(stripped.Signature, undefined);
  assert.equal(stripped.nested.publicKeyPem, undefined);
  assert.equal(stripped.nested.keep, "z");
});

test("cloudEventId is stable for the same db+activity id and differs across activity ids", () => {
  const a = cloudEventId("/db.sqlite", 1, { id: "https://example.test/a" });
  const b = cloudEventId("/db.sqlite", 2, { id: "https://example.test/b" });
  assert.equal(a, cloudEventId("/db.sqlite", 1, { id: "https://example.test/a" }));
  assert.notEqual(a, b);
});

/** A provider's ActivityPub state as a node that moved from SQLite custody to the journal holds it. */
async function movedCustody(work) {
  const { copyFileSync } = await import("node:fs");
  const { createActivityPubJournalCustody } = await import("@red-cup-engineering/activitypub-services-section/journal-custody");
  const { admitSqlActivityPubCustody } = await import("@red-cup-engineering/activitypub-services-section/import-sql-custody");
  const dbPath = join(work, "activitypub.sqlite");
  copyFileSync(FIXTURE_DB_PATH, dbPath);
  const custody = createActivityPubJournalCustody({ stateDirectory: join(work, "activitypub.state") });
  return { dbPath, custody, admitSql: () => admitSqlActivityPubCustody({ databasePath: dbPath, custody }) };
}

test("a cursor from the SQLite era is converted once: nothing emitted twice, nothing lost", async () => {
  const {admitJournalInboxArrivals}=await import("../../admit-set-of-inbox-arrivals.mjs");
  const work = mkdtempSync(join(tmpdir(), "koios-journal-arrivals-"));
  try {
    const { dbPath, custody, admitSql } = await movedCustody(work);
    // The reader emitted the first SQLite row, then custody moved: the journal admitted both rows, and a new arrival came after.
    const sqlEra = admitSetOfInboxArrivals({ dbPath, cursor: { version: 1, lastId: 0 } });
    assert.equal(sqlEra.events.length, 2);
    await admitSql();
    await custody.inbox.retain({ recipient: "koios-news", jsonLd: { type: "Create", id: "https://remote.example/after-the-move" } });
    const converted = convertSqlCursor({ dbPath, cursor: { version: 1, lastId: 1 } });
    assert.deepEqual(converted.events.map((event) => event.data.type), ["Create", "Create"], "the unread SQLite row, then the arrival after the move");
    assert.equal(converted.events[1].data.id, "https://remote.example/after-the-move");
    assert.equal(converted.cursor.version, 2);
    assert.deepEqual(admitJournalInboxArrivals({ dbPath, cursor: JSON.parse(JSON.stringify(converted.cursor)) }).events, [], "the converted cursor reads on from the journal's end");
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
});

test("a cursor that had begun reading the journal converts at its journal offset", async () => {
  
  const work = mkdtempSync(join(tmpdir(), "koios-journal-arrivals-"));
  try {
    const { dbPath, custody, admitSql } = await movedCustody(work);
    await admitSql();
    await custody.inbox.retain({ recipient: "koios-news", jsonLd: { type: "Create", id: "https://remote.example/one" } });
    await custody.inbox.retain({ recipient: "koios-news", jsonLd: { type: "Create", id: "https://remote.example/two" } });
    // Two SQLite rows admitted plus "one" read under the earlier journal-offset cursor.
    const converted = convertSqlCursor({ dbPath, cursor: { version: 1, lastId: 2, journalOffset: 3 } });
    assert.deepEqual(converted.events.map((event) => event.data.id), ["https://remote.example/two"]);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
});

test("arrivals the journal custody retains are emitted once each, read from where the last run stopped", async () => {
  const { admitJournalInboxArrivals, loadCursor: load, saveCursor } = await import("../../admit-set-of-inbox-arrivals.mjs");
  const work = mkdtempSync(join(tmpdir(), "koios-journal-arrivals-"));
  try {
    const { dbPath, custody } = await movedCustody(work);
    const cursorPath = join(work, "cursor.json");
    assert.deepEqual(load(cursorPath), { version: 2, journal: null }, "a node with no cursor reads the journal from its start");
    await custody.inbox.retain({ recipient: "koios-news", jsonLd: { type: "Follow", id: "https://remote.example/f", signature: { value: "x" } } });
    const first = admitJournalInboxArrivals({ dbPath, cursor: load(cursorPath) });
    assert.deepEqual(first.events.map((event) => event.data.type), ["Follow"]);
    assert.equal(first.events[0].data.signature, undefined, "signature material is stripped");
    assert.equal(first.events[0].source, `${KOIOS_ACTOR_URI}/inbox`);
    saveCursor(cursorPath, first.cursor);
    await custody.inbox.retain({ recipient: "koios-news", jsonLd: { type: "Like", id: "https://remote.example/l" } });
    const second = admitJournalInboxArrivals({ dbPath, cursor: load(cursorPath) });
    assert.deepEqual(second.events.map((event) => event.data.type), ["Like"]);
    assert.equal(second.reset, false);
    assert.equal(second.events[0].id, cloudEventId(dbPath, 0, { id: "https://remote.example/l" }), "event ids stay stable across custody media");
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
});
