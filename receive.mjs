// Historical events are private migration evidence, never publication or audience clearance.
import {rowToCloudEvent} from '@causeway-services/activitypub-retained-inbox-provider/inbox-events';
import {readSqlInboxArrivalsSince} from './read-sql.mjs';
/**
 * Historical SQL-to-native cursor conversion is held until exact receiving correspondence exists.
 * The previous receipt-time heuristic could skip or duplicate arrivals.
 * Original intended obligations:
 * what that reader had not yet emitted:
 * - SQLite rows past `lastId` (read once, read-only, by import-sql-inbox-arrivals.mjs);
 * - journal arrivals it had not read: past `journalOffset` arrivals when it had begun reading the
 *   journal, else those retained after the journal custody admitted the SQLite store (the
 *   admission receipt's time, sql-admission.json), which were emitted from SQLite already.
 * @param {object} options
 * @param {string} options.dbPath - the configured ActivityPub SQLite path.
 * @param {{version: 1, lastId: number, journalOffset?: number}} options.cursor - the SQLite-era cursor.
 * @param {string} options.expectedSha256 - independently identified original source.
 * @returns {{events: object[], cursor: {version: 2, journal: object}}}
 */
export function convertSqlCursor() {
  throw new Error("Historical cursor conversion requires independently verified source-to-native receiving correspondence; receipt time is not that correspondence");
}

/**
 * Reads the SQLite arrivals past `cursor.lastId` and maps them to CloudEvents: the SQLite-era
 * reading, kept for a cursor that predates the journal (convertSqlCursor uses the same read).
 * @param {object} options
 * @param {string} options.dbPath - path to the ActivityPub SQLite state file.
 * @param {{lastId?: number}} options.cursor - the prior dedup cursor (no lastId: from the first row).
 * @param {string} options.expectedSha256 - independently identified original source.
 * @returns {{events: object[], cursor: {version: 1, lastId: number}}}
 */
export function admitSetOfInboxArrivals({ dbPath, cursor, expectedSha256 }) {
  const sql = readSqlInboxArrivalsSince({ dbPath, lastId: cursor.lastId ?? 0, expectedSha256 });
  const events = sql.rows.map((row) => rowToCloudEvent({ dbPath, rowId: row.id, activity: row.activity, observedAt: row.observedAt }));
  return { events, cursor: { version: 1, lastId: sql.lastId } };
}

