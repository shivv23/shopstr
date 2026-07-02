import { NostrEvent } from "../types/types";
import { findListingBySlug } from "../url-slugs";
import {
  getDbPool,
  closeDbPool,
  ensureFailedRelayPublishesTable,
} from "./pool";

export { getDbPool, closeDbPool, ensureFailedRelayPublishesTable };

// Queue for serializing cache operations
let cacheQueue: Promise<void> = Promise.resolve();

export async function trackFailedRelayPublishRecord({
  eventId,
  ownerPubkey,
  event,
  relays,
}: {
  eventId: string;
  ownerPubkey: string;
  event: NostrEvent;
  relays: string[];
}): Promise<boolean> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await ensureFailedRelayPublishesTable(client);

    const result = await client.query(
      `INSERT INTO failed_relay_publishes (
         event_id,
         owner_pubkey,
         event_data,
         relays,
         created_at,
         retry_count
       )
       VALUES ($1, $2, $3, $4, $5, 0)
       ON CONFLICT (event_id) DO UPDATE SET
         owner_pubkey = EXCLUDED.owner_pubkey,
         event_data = EXCLUDED.event_data,
         relays = EXCLUDED.relays,
         created_at = EXCLUDED.created_at
       WHERE failed_relay_publishes.owner_pubkey = EXCLUDED.owner_pubkey
       RETURNING event_id`,
      [
        eventId,
        ownerPubkey,
        JSON.stringify(event),
        JSON.stringify(relays),
        Math.floor(Date.now() / 1000),
      ]
    );

    return (result.rowCount ?? 0) > 0;
  } finally {
    if (client) client.release();
  }
}

export async function getFailedRelayPublishesForOwner(ownerPubkey: string) {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await ensureFailedRelayPublishesTable(client);

    const result = await client.query(
      `SELECT event_id, event_data, relays, retry_count
       FROM failed_relay_publishes
       WHERE owner_pubkey = $1
         AND retry_count < 5
         AND event_data IS NOT NULL
       ORDER BY created_at ASC
       LIMIT 50`,
      [ownerPubkey]
    );

    return result.rows
      .filter((row: any) => row.event_data)
      .map((row: any) => {
        try {
          return {
            eventId: row.event_id,
            relays: JSON.parse(row.relays),
            event: JSON.parse(row.event_data),
            retryCount: row.retry_count,
          };
        } catch (error) {
          console.error("Failed to parse row:", row.event_id, error);
          return null;
        }
      })
      .filter(Boolean);
  } finally {
    if (client) client.release();
  }
}

export async function clearFailedRelayPublishForOwner(
  eventId: string,
  ownerPubkey: string
): Promise<void> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await ensureFailedRelayPublishesTable(client);
    await client.query(
      `DELETE FROM failed_relay_publishes
       WHERE event_id = $1 AND owner_pubkey = $2`,
      [eventId, ownerPubkey]
    );
  } finally {
    if (client) client.release();
  }
}

export async function incrementFailedRelayPublishRetryForOwner(
  eventId: string,
  ownerPubkey: string
): Promise<void> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await ensureFailedRelayPublishesTable(client);
    await client.query(
      `UPDATE failed_relay_publishes
       SET retry_count = retry_count + 1
       WHERE event_id = $1 AND owner_pubkey = $2`,
      [eventId, ownerPubkey]
    );
  } finally {
    if (client) client.release();
  }
}

// Map event kinds to table names
export function getTableForKind(kind: number): string | null {
  // Products
  if (kind === 30402) return "product_events";

  // Reviews
  if (kind === 31555) return "review_events";

  // Reports
  if (kind === 1984) return "report_events";

  // Messages
  if (kind === 1059) return "message_events";

  // Profiles
  if (kind === 0 || kind === 30019) return "profile_events";

  // Wallet
  if ([7375, 7376, 17375, 37375].includes(kind)) return "wallet_events";

  // Community
  if ([34550, 1111, 4550].includes(kind)) return "community_events";

  // Config
  if ([10002, 10063, 30405, 30406].includes(kind)) return "config_events";

  return null;
}

// Helper function to check if event kind should only keep latest per pubkey
export function shouldKeepOnlyLatest(kind: number): boolean {
  // Wallet config (17375), wallet state (37375), relay list (10002), blossom servers (10063)
  // User profile (0), shop profile (30019), community definition (34550)
  return [17375, 37375, 10002, 10063, 0, 30019, 34550].includes(kind);
}

// Helper function to check if event is a review (needs special handling per product)
export function isReviewEvent(kind: number): boolean {
  return kind === 31555;
}

export function buildReviewDTagFilter(dTag: string): string {
  return JSON.stringify([["d", dTag]]);
}

// Cache a single event to the database
export async function cacheEvent(event: NostrEvent): Promise<void> {
  const table = getTableForKind(event.kind);
  if (!table) {
    console.warn(`No table mapping for event kind ${event.kind}`);
    return;
  }

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    // For events that should only keep the latest version per pubkey
    if (shouldKeepOnlyLatest(event.kind)) {
      await client.query("BEGIN");

      // Delete older events from the same pubkey with the same kind
      const deleteQuery = {
        text: `DELETE FROM ${table} WHERE pubkey = $1 AND kind = $2`,
        values: [event.pubkey, event.kind] as any[],
      };
      await client.query(deleteQuery);

      // Insert the new event
      const insertQuery = {
        text: `INSERT INTO ${table} (id, pubkey, created_at, kind, tags, content, sig)
               VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        values: [
          event.id,
          event.pubkey,
          event.created_at,
          event.kind,
          JSON.stringify(event.tags),
          event.content,
          event.sig,
        ] as any[],
      };
      await client.query(insertQuery);

      await client.query("COMMIT");
    } else if (isReviewEvent(event.kind)) {
      // For reviews, keep only the latest per pubkey per product
      await client.query("BEGIN");

      // Extract the product identifier from the 'd' tag (format: "30402:merchant_pubkey:product_d_tag")
      const dTag = event.tags.find((tag) => tag[0] === "d")?.[1];

      if (dTag) {
        // Delete older reviews from the same pubkey for the same product
        const deleteQuery = {
          text: `DELETE FROM ${table} WHERE pubkey = $1 AND kind = $2 AND tags @> $3::jsonb`,
          values: [
            event.pubkey,
            event.kind,
            buildReviewDTagFilter(dTag),
          ] as any[],
        };
        await client.query(deleteQuery);
      }

      // Insert the new review
      const insertQuery = {
        text: `INSERT INTO ${table} (id, pubkey, created_at, kind, tags, content, sig)
               VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        values: [
          event.id,
          event.pubkey,
          event.created_at,
          event.kind,
          JSON.stringify(event.tags),
          event.content,
          event.sig,
        ] as any[],
      };
      await client.query(insertQuery);

      await client.query("COMMIT");
    } else {
      // For other events, use the normal upsert behavior
      const query = {
        text: `INSERT INTO ${table} (id, pubkey, created_at, kind, tags, content, sig)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (id) DO UPDATE SET
                 pubkey = EXCLUDED.pubkey,
                 created_at = EXCLUDED.created_at,
                 tags = EXCLUDED.tags,
                 content = EXCLUDED.content,
                 sig = EXCLUDED.sig,
                 cached_at = CURRENT_TIMESTAMP`,
        values: [
          event.id,
          event.pubkey,
          event.created_at,
          event.kind,
          JSON.stringify(event.tags),
          event.content,
          event.sig,
        ] as any[],
      };
      await client.query(query);
    }
  } catch (error) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("Failed to rollback transaction:", rollbackError);
      }
    }
    console.error("Failed to cache event %s:", event.id, error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Cache multiple events in a batch with retry logic for deadlocks
export async function cacheEvents(events: NostrEvent[]): Promise<void> {
  if (events.length === 0) return;

  // Queue the operation to prevent overwhelming the pool
  return new Promise((resolve, reject) => {
    cacheQueue = cacheQueue
      .then(async () => {
        const maxRetries = 3;
        let attempt = 0;

        while (attempt < maxRetries) {
          try {
            await cacheEventsTransaction(events);
            resolve();
            return;
          } catch (error: any) {
            const isDeadlock = error?.code === "40P01";
            const isConnectionError =
              error?.message?.includes("Connection terminated") ||
              error?.message?.includes("Connection timeout");

            if ((isDeadlock || isConnectionError) && attempt < maxRetries - 1) {
              attempt++;
              const delay = 100 * Math.pow(2, attempt);
              await new Promise((res) => setTimeout(res, delay));
            } else {
              reject(error);
              return;
            }
          }
        }
      })
      .catch(reject);
  });
}

// Internal function to perform the actual transaction
async function cacheEventsTransaction(events: NostrEvent[]): Promise<void> {
  const eventsByTable = new Map<string, NostrEvent[]>();

  // Group events by table
  for (const event of events) {
    const table = getTableForKind(event.kind);
    if (table) {
      if (!eventsByTable.has(table)) {
        eventsByTable.set(table, []);
      }
      eventsByTable.get(table)!.push(event);
    }
  }

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await client.query("BEGIN");

    for (const [table, tableEvents] of eventsByTable.entries()) {
      // Group events by type
      const latestOnlyEvents = tableEvents.filter((e) =>
        shouldKeepOnlyLatest(e.kind)
      );
      const reviewEvents = tableEvents.filter((e) => isReviewEvent(e.kind));
      const regularEvents = tableEvents.filter(
        (e) => !shouldKeepOnlyLatest(e.kind) && !isReviewEvent(e.kind)
      );

      // Handle latest-only events (per pubkey) - batch by pubkey+kind to reduce queries
      const latestByPubkeyKind = new Map<string, NostrEvent>();
      for (const event of latestOnlyEvents) {
        const key = `${event.pubkey}:${event.kind}`;
        const existing = latestByPubkeyKind.get(key);
        if (!existing || event.created_at > existing.created_at) {
          latestByPubkeyKind.set(key, event);
        }
      }

      for (const event of latestByPubkeyKind.values()) {
        // First, lock and delete old rows
        await client.query(
          `DELETE FROM ${table} WHERE pubkey = $1 AND kind = $2 AND id != $3`,
          [event.pubkey, event.kind, event.id] as any[]
        );

        // Then insert/update with ON CONFLICT
        const upsertQuery = {
          text: `
            INSERT INTO ${table} (id, pubkey, created_at, kind, tags, content, sig)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
              pubkey = EXCLUDED.pubkey,
              created_at = EXCLUDED.created_at,
              tags = EXCLUDED.tags,
              content = EXCLUDED.content,
              sig = EXCLUDED.sig,
              cached_at = CURRENT_TIMESTAMP
          `,
          values: [
            event.id,
            event.pubkey,
            event.created_at,
            event.kind,
            JSON.stringify(event.tags),
            event.content,
            event.sig,
          ] as any[],
        };
        await client.query(upsertQuery);
      }

      // Handle review events (latest per pubkey per product) - batch by pubkey+dtag
      const latestReviewByPubkeyDtag = new Map<string, NostrEvent>();
      for (const event of reviewEvents) {
        const dTag = event.tags.find((tag) => tag[0] === "d")?.[1];
        if (dTag) {
          const key = `${event.pubkey}:${dTag}`;
          const existing = latestReviewByPubkeyDtag.get(key);
          if (!existing || event.created_at > existing.created_at) {
            latestReviewByPubkeyDtag.set(key, event);
          }
        }
      }

      for (const event of latestReviewByPubkeyDtag.values()) {
        const dTag = event.tags.find((tag) => tag[0] === "d")?.[1];

        if (dTag) {
          // First, lock and delete old rows
          await client.query(
            `DELETE FROM ${table} WHERE pubkey = $1 AND kind = $2 AND tags @> $3::jsonb AND id != $4`,
            [
              event.pubkey,
              event.kind,
              buildReviewDTagFilter(dTag),
              event.id,
            ] as any[]
          );

          // Then insert/update with ON CONFLICT
          const upsertQuery = {
            text: `
              INSERT INTO ${table} (id, pubkey, created_at, kind, tags, content, sig)
              VALUES ($1, $2, $3, $4, $5, $6, $7)
              ON CONFLICT (id) DO UPDATE SET
                pubkey = EXCLUDED.pubkey,
                created_at = EXCLUDED.created_at,
                tags = EXCLUDED.tags,
                content = EXCLUDED.content,
                sig = EXCLUDED.sig,
                cached_at = CURRENT_TIMESTAMP
            `,
            values: [
              event.id,
              event.pubkey,
              event.created_at,
              event.kind,
              JSON.stringify(event.tags),
              event.content,
              event.sig,
            ] as any[],
          };
          await client.query(upsertQuery);
        }
      }

      // Handle regular events with upsert
      for (const event of regularEvents) {
        const query = {
          text: `INSERT INTO ${table} (id, pubkey, created_at, kind, tags, content, sig)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (id) DO UPDATE SET
                   pubkey = EXCLUDED.pubkey,
                   created_at = EXCLUDED.created_at,
                   tags = EXCLUDED.tags,
                   content = EXCLUDED.content,
                   sig = EXCLUDED.sig,
                   cached_at = CURRENT_TIMESTAMP`,
          values: [
            event.id,
            event.pubkey,
            event.created_at,
            event.kind,
            JSON.stringify(event.tags),
            event.content,
            event.sig,
          ] as any[],
        };
        await client.query(query);
      }
    }

    await client.query("COMMIT");
  } catch (error) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("Failed to rollback transaction:", rollbackError);
      }
    }
    console.error("Failed to cache events batch:", error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fetch events from cache by kind and optional filters
export async function fetchCachedEvents(
  kind: number,
  filters?: {
    pubkey?: string;
    limit?: number;
    offset?: number;
    since?: number;
    until?: number;
  }
): Promise<NostrEvent[]> {
  const table = getTableForKind(kind);
  if (!table) return [];

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    let query = `SELECT id, pubkey, created_at, kind, tags, content, sig FROM ${table} WHERE kind = $1`;
    const params: any[] = [kind];
    let paramIndex = 2;

    if (filters?.pubkey) {
      query += ` AND pubkey = $${paramIndex++}`;
      params.push(filters.pubkey);
    }

    if (filters?.since) {
      query += ` AND created_at >= $${paramIndex++}`;
      params.push(filters.since);
    }

    if (filters?.until) {
      query += ` AND created_at <= $${paramIndex++}`;
      params.push(filters.until);
    }

    query += " ORDER BY created_at DESC";

    if (filters?.limit) {
      query += ` LIMIT $${paramIndex++}`;
      params.push(filters.limit);
    }

    if (filters?.offset) {
      query += ` OFFSET $${paramIndex++}`;
      params.push(filters.offset);
    }

    const result = await client.query(query, params);

    return result.rows.map((row) => ({
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    }));
  } catch (error) {
    console.error("Failed to fetch cached events:", error);
    return [];
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Delete cached event by ID
export async function deleteCachedEvent(
  eventId: string,
  kind: number
): Promise<void> {
  const table = getTableForKind(kind);
  if (!table) return;

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await client.query(`DELETE FROM ${table} WHERE id = $1`, [eventId]);
  } catch (error) {
    console.error(`Failed to delete cached event ${eventId}:`, error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Delete cached events by IDs across all tables
export async function deleteCachedEventsByIds(
  eventIds: string[]
): Promise<void> {
  if (eventIds.length === 0) return;

  const dbPool = getDbPool();
  let client;

  // All tables that can store events
  const otherTables = [
    "review_events",
    "report_events",
    "message_events",
    "profile_events",
    "wallet_events",
    "community_events",
    "config_events",
  ];

  try {
    client = await dbPool.connect();
    await client.query("BEGIN");

    // For product_events: delete by ID, and also remove any versions that are
    // strictly OLDER than the deleted event (same pubkey + d-tag, smaller
    // created_at). This cleans up stale versions without touching a newer
    // version that may already exist (e.g. an edit published before the old
    // one was explicitly removed).
    await client.query(
      `WITH refs AS (
         SELECT
           ref.pubkey,
           ref.created_at,
           d.d_tag
         FROM product_events ref
         CROSS JOIN LATERAL (
           SELECT elem->>1 AS d_tag
           FROM jsonb_array_elements(ref.tags) elem
           WHERE elem->>0 = 'd'
           LIMIT 1
         ) d
         WHERE ref.id = ANY($1)
       )
       DELETE FROM product_events pe
       WHERE pe.id = ANY($1)
         OR EXISTS (
           SELECT 1
           FROM refs
           WHERE pe.kind = 30402
             AND pe.pubkey = refs.pubkey
             AND pe.created_at < refs.created_at
             AND EXISTS (
               SELECT 1
               FROM jsonb_array_elements(pe.tags) elem
               WHERE elem->>0 = 'd'
                 AND elem->>1 = refs.d_tag
             )
         )`,
      [eventIds]
    );

    // For all other tables, delete by ID only
    for (const table of otherTables) {
      await client.query(`DELETE FROM ${table} WHERE id = ANY($1)`, [eventIds]);
    }

    await client.query("COMMIT");
  } catch (error) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("Failed to rollback transaction:", rollbackError);
      }
    }
    console.error("Failed to delete cached events:", error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function cachedEventsBelongToPubkey(
  eventIds: string[],
  pubkey: string
): Promise<boolean> {
  if (eventIds.length === 0) return true;

  const uniqueEventIds = Array.from(new Set(eventIds));

  const dbPool = getDbPool();
  let client;

  const eventTables = [
    "product_events",
    "review_events",
    "message_events",
    "profile_events",
    "wallet_events",
    "community_events",
    "config_events",
  ];

  try {
    client = await dbPool.connect();
    const unionQuery = eventTables
      .map((table) => `SELECT id, pubkey FROM ${table} WHERE id = ANY($1)`)
      .join(" UNION ALL ");

    const result = await client.query<{ id: string; pubkey: string }>(
      unionQuery,
      [uniqueEventIds]
    );

    // Fail closed: every requested ID must exist somewhere in the cache AND
    // every row found for those IDs must belong to the caller. An unknown ID
    // (no row in any table) is treated as not-owned so the route refuses the
    // whole batch instead of silently succeeding.
    const ownedIds = new Set<string>();
    for (const row of result.rows) {
      if (row.pubkey !== pubkey) {
        return false;
      }
      ownedIds.add(row.id);
    }

    return uniqueEventIds.every((id) => ownedIds.has(id));
  } catch (error) {
    console.error("Failed to verify cached event ownership:", error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fetch all products from database, returning only the latest version per
// (pubkey, d-tag) so that updated or deleted-then-re-listed products never
// show stale duplicates.
export async function fetchAllProductsFromDb(
  limit = 500,
  offset = 0
): Promise<NostrEvent[]> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();

    // Inner query: DISTINCT ON (pubkey, d_tag) with ORDER BY created_at DESC
    // selects the single newest event per listing address.
    // Outer query re-sorts by created_at DESC so callers get recent products
    // first, then applies LIMIT/OFFSET for batched pagination.
    const result = await client.query(
      `SELECT pe.id, pe.pubkey, pe.created_at, pe.kind,
              pe.tags, pe.content, pe.sig
       FROM (
         SELECT DISTINCT ON (p.pubkey, d.d_tag)
           p.id, p.pubkey, p.created_at, p.kind, p.tags, p.content, p.sig
         FROM product_events p,
         LATERAL (
           SELECT COALESCE(
             (SELECT elem->>1
              FROM jsonb_array_elements(p.tags) elem
              WHERE elem->>0 = 'd'
              LIMIT 1),
             p.id
           ) AS d_tag
         ) d
         WHERE p.kind = 30402
         ORDER BY p.pubkey, d.d_tag, p.created_at DESC
       ) pe
       ORDER BY pe.created_at DESC
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    return result.rows.map((row) => ({
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    }));
  } catch (error) {
    console.error("Failed to fetch products from database:", error);
    return [];
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fetch all reviews from database
export async function fetchAllReviewsFromDb(): Promise<NostrEvent[]> {
  return fetchCachedEvents(31555);
}

export async function fetchRelevantReportsFromDb(
  productIds: string[],
  profilePubkeys: string[],
  limit = 500
): Promise<NostrEvent[]> {
  if (productIds.length === 0 && profilePubkeys.length === 0) {
    return [];
  }

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const clauses: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    if (profilePubkeys.length > 0) {
      clauses.push(`
        EXISTS (
          SELECT 1
          FROM jsonb_array_elements(tags) elem
          WHERE elem->>0 = 'p' AND elem->>1 = ANY($${paramIndex++})
        )
      `);
      params.push(profilePubkeys);
    }

    if (productIds.length > 0) {
      clauses.push(`
        EXISTS (
          SELECT 1
          FROM jsonb_array_elements(tags) elem
          WHERE elem->>0 = 'e' AND elem->>1 = ANY($${paramIndex++})
        )
      `);
      params.push(productIds);
    }

    const boundedLimit = Math.max(1, Math.min(limit, 500));
    params.push(boundedLimit);

    const result = await client.query(
      `SELECT id, pubkey, created_at, kind, tags, content, sig
       FROM report_events
       WHERE ${clauses.join(" OR ")}
       ORDER BY created_at DESC
       LIMIT $${paramIndex}`,
      params
    );

    return result.rows.map((row) => ({
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    }));
  } catch (error) {
    console.error("Failed to fetch relevant reports from database:", error);
    return [];
  } finally {
    if (client) client.release();
  }
}

// Fetch all messages from database with read status
export async function fetchAllMessagesFromDb(
  pubkey?: string
): Promise<(NostrEvent & { is_read: boolean })[]> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    let query = `SELECT id, pubkey, created_at, kind, tags, content, sig, COALESCE(is_read, FALSE) as is_read 
                 FROM message_events WHERE 1=1`;
    const params: any[] = [];
    let paramIndex = 1;

    if (pubkey) {
      query += ` AND EXISTS (SELECT 1 FROM jsonb_array_elements(tags) elem WHERE elem->>0 = 'p' AND elem->>1 = $${paramIndex++})`;
      params.push(pubkey);
    }

    query += " ORDER BY created_at DESC";

    const result = await client.query(query, params);

    return result.rows.map((row) => ({
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
      is_read: row.is_read,
    }));
  } catch (error) {
    console.error("Failed to fetch messages from database:", error);
    return [];
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Mark messages as read in database
export async function markMessagesAsRead(
  messageIds: string[],
  pubkey: string
): Promise<void> {
  if (messageIds.length === 0) return;

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await client.query(
      `UPDATE message_events
       SET is_read = TRUE
       WHERE id = ANY($1)
       AND (
         pubkey = $2
         OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(tags) elem
           WHERE elem->>0 = 'p' AND elem->>1 = $2
         )
       )`,
      [messageIds, pubkey] as any[]
    );
  } catch (error) {
    console.error("Failed to mark messages as read:", error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Get unread message count for a user
export async function getUnreadMessageCount(pubkey: string): Promise<number> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT COUNT(*)
       FROM message_events
       WHERE (
         pubkey = $1
         OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(tags) elem
           WHERE elem->>0 = 'p' AND elem->>1 = $1
         )
       )
       AND (is_read = FALSE OR is_read IS NULL)`,
      [pubkey]
    );
    return parseInt(result.rows[0].count, 10);
  } catch (error) {
    console.error("Failed to get unread message count:", error);
    return 0;
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function getOrderParticipants(orderId: string): Promise<{
  buyerPubkey: string | null;
  sellerPubkey: string | null;
}> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const result = await client.query<{ tags: string[][] }>(
      `SELECT tags
       FROM message_events
       WHERE order_id = $1
       ORDER BY created_at DESC`,
      [orderId] as any[]
    );

    let buyerPubkey: string | null = null;
    let sellerPubkey: string | null = null;

    for (const row of result.rows) {
      const tags = Array.isArray(row.tags) ? (row.tags as string[][]) : [];

      if (!buyerPubkey) {
        const buyerTag = tags.find((tag) => tag[0] === "b");
        if (buyerTag?.[1]) {
          buyerPubkey = buyerTag[1];
        }
      }

      if (!sellerPubkey) {
        const itemTag = tags.find((tag) => tag[0] === "item");
        const productAddress =
          tags.find((tag) => tag[0] === "a")?.[1] || itemTag?.[1];
        const addressParts = productAddress?.split(":");
        if (addressParts && addressParts.length >= 2 && addressParts[1]) {
          sellerPubkey = addressParts[1];
        }
      }

      if (buyerPubkey && sellerPubkey) {
        break;
      }
    }

    return { buyerPubkey, sellerPubkey };
  } catch (error) {
    console.error("Failed to get order participants:", error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Update order status in database
export async function updateOrderStatus(
  orderId: string,
  status: string,
  pubkey: string,
  messageId?: string
): Promise<void> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();

    if (messageId) {
      await client.query(
        `UPDATE message_events
         SET order_status = $1
         WHERE id = $2
         AND order_id = $3
         AND (
           pubkey = $4
           OR EXISTS (
             SELECT 1
             FROM jsonb_array_elements(tags) elem
             WHERE elem->>0 = 'p' AND elem->>1 = $4
           )
         )`,
        [status, messageId, orderId, pubkey]
      );
    }

    await client.query(
      `UPDATE message_events
       SET order_status = $1
       WHERE order_id = $2
       AND (
         pubkey = $3
         OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(tags) elem
           WHERE elem->>0 = 'p' AND elem->>1 = $3
         )
       )`,
      [status, orderId, pubkey]
    );
  } catch (error) {
    console.error("Failed to update order status:", error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Get order statuses from database
export async function getOrderStatuses(
  orderIds: string[]
): Promise<Record<string, string>> {
  if (orderIds.length === 0) return {};

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();

    const result = await client.query(
      `SELECT DISTINCT ON (order_id) order_id, order_status 
       FROM message_events 
       WHERE order_id = ANY($1) AND order_status IS NOT NULL
       ORDER BY order_id, created_at DESC`,
      [orderIds]
    );

    const statuses: Record<string, string> = {};
    for (const row of result.rows) {
      if (row.order_id && row.order_status) {
        statuses[row.order_id] = row.order_status;
      }
    }

    return statuses;
  } catch (error) {
    console.error("Failed to get order statuses:", error);
    return {};
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fetch all profiles from database (both user and shop profiles)
export async function fetchAllProfilesFromDb(): Promise<NostrEvent[]> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const query = `SELECT id, pubkey, created_at, kind, tags, content, sig 
                   FROM profile_events 
                   ORDER BY created_at DESC`;

    const result = await client.query(query);

    return result.rows.map((row) => ({
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    }));
  } catch (error) {
    console.error("Failed to fetch profiles from database:", error);
    return [];
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fetch wallet events from database
export async function fetchAllWalletEventsFromDb(
  pubkey: string
): Promise<NostrEvent[]> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const query = `SELECT id, pubkey, created_at, kind, tags, content, sig 
                   FROM wallet_events 
                   WHERE pubkey = $1
                   ORDER BY created_at DESC`;

    const result = await client.query(query, [pubkey]);

    return result.rows.map((row) => ({
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    }));
  } catch (error) {
    console.error("Failed to fetch wallet events from database:", error);
    return [];
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fetch all communities from database
export async function fetchAllCommunitiesFromDb(): Promise<NostrEvent[]> {
  return fetchCachedEvents(34550);
}

// Fetch relay config events from database
export async function fetchRelayConfigFromDb(
  pubkey: string
): Promise<NostrEvent[]> {
  return fetchCachedEvents(10002, { pubkey });
}

// Fetch blossom server config from database
export async function fetchBlossomConfigFromDb(
  pubkey: string
): Promise<NostrEvent[]> {
  return fetchCachedEvents(10063, { pubkey });
}

// Fetch buyer P2PK escrow records from database
export async function fetchEscrowRecordsFromDb(
  pubkey: string
): Promise<NostrEvent[]> {
  return fetchCachedEvents(30406, { pubkey });
}

// Add discount code
export async function addDiscountCode(
  code: string,
  pubkey: string,
  discountPercentage: number,
  expiration?: number
): Promise<void> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const query = {
      text: `INSERT INTO discount_codes (code, pubkey, discount_percentage, expiration)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (code, pubkey) DO UPDATE SET
               discount_percentage = EXCLUDED.discount_percentage,
               expiration = EXCLUDED.expiration`,
      values: [code, pubkey, discountPercentage, expiration ?? null] as any[],
    };
    await client.query(query);
  } catch (error) {
    console.error("Failed to add discount code:", error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Get discount codes for a merchant
export async function getDiscountCodesByPubkey(pubkey: string): Promise<
  Array<{
    code: string;
    discount_percentage: number;
    expiration: number | null;
  }>
> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT code, discount_percentage, expiration FROM discount_codes WHERE pubkey = $1 ORDER BY created_at DESC`,
      [pubkey]
    );
    return result.rows.map((row) => ({
      code: row.code,
      discount_percentage: Number(row.discount_percentage),
      expiration: row.expiration === null ? null : Number(row.expiration),
    }));
  } catch (error) {
    console.error("Failed to fetch discount codes:", error);
    return [];
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Validate and get discount code
export async function validateDiscountCode(
  code: string,
  pubkey: string
): Promise<{ valid: boolean; discount_percentage?: number }> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT discount_percentage, expiration FROM discount_codes WHERE code = $1 AND pubkey = $2`,
      [code, pubkey]
    );

    if (result.rows.length === 0) {
      return { valid: false };
    }

    const { discount_percentage, expiration } = result.rows[0];

    // Check if code is expired
    if (expiration && Date.now() / 1000 > expiration) {
      return { valid: false };
    }

    return { valid: true, discount_percentage: Number(discount_percentage) };
  } catch (error) {
    console.error("Failed to validate discount code:", error);
    return { valid: false };
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Delete discount code
export async function deleteDiscountCode(
  code: string,
  pubkey: string
): Promise<void> {
  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();
    await client.query(
      `DELETE FROM discount_codes WHERE code = $1 AND pubkey = $2`,
      [code, pubkey]
    );
  } catch (error) {
    console.error("Failed to delete discount code:", error);
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Marketplace stats: listing count + distinct seller count
export async function fetchMarketplaceStats(): Promise<{
  listingCount: number;
  sellerCount: number;
}> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query<{
      listing_count: string;
      seller_count: string;
    }>(
      `SELECT COUNT(*) AS listing_count, COUNT(DISTINCT pubkey) AS seller_count FROM product_events`
    );
    const row = result.rows[0];
    return {
      listingCount: parseInt(row?.listing_count ?? "0", 10) || 0,
      sellerCount: parseInt(row?.seller_count ?? "0", 10) || 0,
    };
  } catch (err) {
    console.error("fetchMarketplaceStats error:", err);
    return { listingCount: 0, sellerCount: 0 };
  } finally {
    if (client) client.release();
  }
}

export function profileNameToSlug(name: string): string {
  if (!name) return "";
  return name
    .trim()
    .replace(/[#?&\/\\%=+<>{}|^~\[\]`@!$*()"';:,]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

export async function fetchProductByIdFromDb(
  id: string
): Promise<NostrEvent | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT id, pubkey, created_at, kind, tags, content, sig
       FROM product_events WHERE id = $1 LIMIT 1`,
      [id]
    );
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    };
  } catch (error) {
    console.error("Failed to fetch product by id:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}

export async function fetchProductByDTagAndPubkey(
  dTag: string,
  pubkey: string
): Promise<NostrEvent | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT id, pubkey, created_at, kind, tags, content, sig
       FROM product_events
       WHERE pubkey = $1
         AND EXISTS (
           SELECT 1 FROM jsonb_array_elements(tags) t
           WHERE t->>0 = 'd' AND t->>1 = $2
         )
       ORDER BY created_at DESC LIMIT 1`,
      [pubkey, dTag]
    );
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    };
  } catch (error) {
    console.error("Failed to fetch product by d-tag and pubkey:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}

export async function fetchProductByListingSlug(
  slug: string
): Promise<NostrEvent | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT id, pubkey, created_at, kind, tags, content, sig
       FROM product_events
       WHERE EXISTS (
         SELECT 1 FROM jsonb_array_elements(tags) t WHERE t->>0 = 'title'
       )
       ORDER BY created_at DESC`
    );
    const matchingRow = findListingBySlug(
      slug,
      result.rows
        .map((row) => {
          const tags: string[][] = row.tags;
          const titleTag = tags.find((t) => t[0] === "title");
          const title = titleTag?.[1];

          if (!title) {
            return null;
          }

          return {
            row,
            id: row.id,
            pubkey: row.pubkey,
            title,
          };
        })
        .filter(
          (
            candidate
          ): candidate is {
            row: (typeof result.rows)[number];
            id: string;
            pubkey: string;
            title: string;
          } => candidate !== null
        )
    );

    if (!matchingRow) return null;

    const row = matchingRow.row;
    return {
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    };
  } catch (error) {
    console.error("Failed to fetch product by listing slug:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}

export async function fetchShopProfileByPubkeyFromDb(
  pubkey: string
): Promise<NostrEvent | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT id, pubkey, created_at, kind, tags, content, sig
       FROM profile_events
       WHERE pubkey = $1 AND kind = 30019
       ORDER BY created_at DESC LIMIT 1`,
      [pubkey]
    );
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    };
  } catch (error) {
    console.error("Failed to fetch shop profile by pubkey:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}

export async function fetchProfilePubkeyByNameSlug(
  nameSlug: string
): Promise<string | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT pubkey, content FROM profile_events WHERE kind = 0 ORDER BY created_at DESC`
    );
    const pubkeySuffixMatch = nameSlug.match(/^(.+)-([a-f0-9]{8})$/);
    const baseSlug = pubkeySuffixMatch?.[1];
    const pubkeyFragment = pubkeySuffixMatch?.[2];

    let exactMatch: string | null = null;
    let exactMatchCount = 0;
    let disambiguatedMatch: string | null = null;

    for (const row of result.rows) {
      let profileName: string | undefined;
      try {
        const content = JSON.parse(row.content);
        profileName = content.name;
      } catch {
        continue;
      }
      if (!profileName) continue;
      const slug = profileNameToSlug(profileName);

      if (slug === nameSlug) {
        exactMatchCount += 1;
        if (exactMatchCount > 1) {
          return null;
        }

        exactMatch = row.pubkey;
      }

      if (
        !exactMatch &&
        !disambiguatedMatch &&
        baseSlug &&
        pubkeyFragment &&
        slug === baseSlug &&
        row.pubkey.startsWith(pubkeyFragment)
      ) {
        disambiguatedMatch = row.pubkey;
      }
    }

    if (exactMatch) {
      return exactMatch;
    }

    return disambiguatedMatch;
  } catch (error) {
    console.error("Failed to fetch profile pubkey by name slug:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}

export async function fetchShopPubkeyBySlug(
  slug: string
): Promise<string | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT pubkey FROM shop_slugs WHERE slug = $1 LIMIT 1`,
      [slug.toLowerCase().trim()]
    );
    if (result.rows.length === 0) return null;
    return result.rows[0].pubkey;
  } catch (error) {
    console.error("Failed to fetch shop pubkey by slug:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}

export async function fetchCommunityByPubkeyAndIdentifier(
  pubkey: string,
  identifier: string
): Promise<NostrEvent | null> {
  const dbPool = getDbPool();
  let client;
  try {
    client = await dbPool.connect();
    const result = await client.query(
      `SELECT id, pubkey, created_at, kind, tags, content, sig
       FROM community_events
       WHERE pubkey = $1 AND kind = 34550
         AND EXISTS (
           SELECT 1 FROM jsonb_array_elements(tags) t
           WHERE t->>0 = 'd' AND t->>1 = $2
         )
       ORDER BY created_at DESC LIMIT 1`,
      [pubkey, identifier]
    );
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
      id: row.id,
      pubkey: row.pubkey,
      created_at: row.created_at,
      kind: row.kind,
      tags: row.tags,
      content: row.content,
      sig: row.sig,
    };
  } catch (error) {
    console.error("Failed to fetch community by pubkey and identifier:", error);
    return null;
  } finally {
    if (client) client.release();
  }
}
