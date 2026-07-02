import { Pool, PoolClient } from "pg";

let pool: Pool | null = null;
let tablesInitialized = false;
let initializingTables = false;

export async function ensureFailedRelayPublishesTable(
  client: PoolClient
): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS failed_relay_publishes (
      event_id TEXT PRIMARY KEY,
      owner_pubkey TEXT,
      event_data TEXT NOT NULL,
      relays TEXT NOT NULL,
      created_at BIGINT NOT NULL,
      retry_count INTEGER DEFAULT 0
    )
  `);

  await client.query(`
    ALTER TABLE failed_relay_publishes
    ADD COLUMN IF NOT EXISTS event_data TEXT
  `);

  await client.query(`
    ALTER TABLE failed_relay_publishes
    ADD COLUMN IF NOT EXISTS owner_pubkey TEXT
  `);

  await client.query(`
    DELETE FROM failed_relay_publishes
    WHERE owner_pubkey IS NULL
  `);
}

export function getDbPool(): Pool {
  if (!pool) {
    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
      throw new Error("DATABASE_URL environment variable is not set");
    }

    const poolUrl = databaseUrl.replace(".us-east-2", "-pooler.us-east-2");
    pool = new Pool({
      connectionString: poolUrl,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 20000,
      allowExitOnIdle: true,
    });

    pool.on("error", (err) => {
      console.error("Unexpected error on idle database client", err);
    });

    if (!tablesInitialized && !initializingTables) {
      initializingTables = true;
      initializeTables().catch((error) => {
        console.error("Failed to initialize database tables:", error);
        initializingTables = false;
      });
    }
  }
  return pool;
}

async function initializeTables(): Promise<void> {
  if (tablesInitialized) return;

  const dbPool = getDbPool();
  let client;

  try {
    client = await dbPool.connect();

    await client.query(`
      CREATE TABLE IF NOT EXISTS product_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT product_events_kind_check CHECK (kind = 30402)
      );

      CREATE INDEX IF NOT EXISTS idx_product_events_pubkey ON product_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_product_events_created_at ON product_events(created_at DESC);

      CREATE TABLE IF NOT EXISTS review_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT review_events_kind_check CHECK (kind = 31555)
      );

      CREATE INDEX IF NOT EXISTS idx_review_events_pubkey ON review_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_review_events_tags ON review_events USING gin (tags jsonb_path_ops);

      CREATE TABLE IF NOT EXISTS report_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT report_events_kind_check CHECK (kind = 1984)
      );

      CREATE INDEX IF NOT EXISTS idx_report_events_pubkey ON report_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_report_events_created_at ON report_events(created_at DESC);

      CREATE TABLE IF NOT EXISTS message_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          is_read BOOLEAN DEFAULT FALSE,
          order_status TEXT DEFAULT NULL,
          order_id TEXT DEFAULT NULL,
          CONSTRAINT message_events_kind_check CHECK (kind = 1059)
      );

      CREATE INDEX IF NOT EXISTS idx_message_events_pubkey ON message_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_message_events_created_at ON message_events(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_message_events_tags_p ON message_events USING gin (tags jsonb_path_ops);

      CREATE TABLE IF NOT EXISTS profile_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT profile_events_kind_check CHECK (kind IN (0, 30019))
      );

      CREATE INDEX IF NOT EXISTS idx_profile_events_pubkey ON profile_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_profile_events_kind ON profile_events(kind);

      CREATE TABLE IF NOT EXISTS wallet_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT wallet_events_kind_check CHECK (kind IN (7375, 7376, 17375, 37375))
      );

      CREATE INDEX IF NOT EXISTS idx_wallet_events_pubkey ON wallet_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_wallet_events_kind ON wallet_events(kind);

      CREATE TABLE IF NOT EXISTS community_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT community_events_kind_check CHECK (kind IN (34550, 1111, 4550))
      );

      CREATE INDEX IF NOT EXISTS idx_community_events_pubkey ON community_events(pubkey);
      CREATE INDEX IF NOT EXISTS idx_community_events_kind ON community_events(kind);

      CREATE TABLE IF NOT EXISTS config_events (
          id TEXT PRIMARY KEY,
          pubkey TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          kind INTEGER NOT NULL,
          tags JSONB NOT NULL,
          content TEXT NOT NULL,
          sig TEXT NOT NULL,
          cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT config_events_kind_check CHECK (kind IN (10002, 10063, 30405, 30406))
      );

      CREATE INDEX IF NOT EXISTS idx_config_events_pubkey ON config_events(pubkey);

      CREATE TABLE IF NOT EXISTS discount_codes (
          id SERIAL PRIMARY KEY,
          code TEXT NOT NULL,
          pubkey TEXT NOT NULL,
          discount_percentage DECIMAL(5,2) NOT NULL CHECK (discount_percentage > 0 AND discount_percentage <= 100),
          expiration BIGINT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(code, pubkey)
      );

      CREATE INDEX IF NOT EXISTS idx_discount_codes_pubkey ON discount_codes(pubkey);
      CREATE INDEX IF NOT EXISTS idx_discount_codes_code ON discount_codes(code);

      CREATE TABLE IF NOT EXISTS mcp_api_keys (
          id SERIAL PRIMARY KEY,
          key_prefix TEXT NOT NULL,
          key_hash TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
          pubkey TEXT NOT NULL,
          permissions TEXT NOT NULL DEFAULT 'read' CHECK (permissions IN ('read', 'read_write')),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          last_used_at TIMESTAMP,
          is_active BOOLEAN DEFAULT TRUE
      );

      CREATE INDEX IF NOT EXISTS idx_mcp_api_keys_key_hash ON mcp_api_keys(key_hash);
      CREATE INDEX IF NOT EXISTS idx_mcp_api_keys_pubkey ON mcp_api_keys(pubkey);

      CREATE TABLE IF NOT EXISTS mcp_orders (
          id SERIAL PRIMARY KEY,
          order_id TEXT NOT NULL UNIQUE,
          api_key_id INTEGER REFERENCES mcp_api_keys(id),
          buyer_pubkey TEXT NOT NULL,
          seller_pubkey TEXT NOT NULL,
          product_id TEXT NOT NULL,
          product_title TEXT,
          quantity INTEGER NOT NULL DEFAULT 1,
          amount_total NUMERIC(12,2) NOT NULL,
          currency TEXT NOT NULL DEFAULT 'sats',
          shipping_address JSONB,
          payment_ref TEXT,
          payment_status TEXT NOT NULL DEFAULT 'pending' CHECK (payment_status IN ('pending', 'processing', 'paid', 'failed', 'refunded')),
          order_status TEXT NOT NULL DEFAULT 'pending' CHECK (order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_mcp_orders_order_id ON mcp_orders(order_id);
      CREATE INDEX IF NOT EXISTS idx_mcp_orders_buyer_pubkey ON mcp_orders(buyer_pubkey);
      CREATE INDEX IF NOT EXISTS idx_mcp_orders_seller_pubkey ON mcp_orders(seller_pubkey);
      CREATE INDEX IF NOT EXISTS idx_mcp_orders_api_key_id ON mcp_orders(api_key_id);

      CREATE TABLE IF NOT EXISTS shop_slugs (
          pubkey TEXT PRIMARY KEY,
          slug TEXT NOT NULL UNIQUE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_shop_slugs_slug ON shop_slugs(slug);

      CREATE TABLE IF NOT EXISTS custom_domains (
          pubkey TEXT PRIMARY KEY,
          domain TEXT NOT NULL UNIQUE,
          shop_slug TEXT NOT NULL,
          verified BOOLEAN DEFAULT FALSE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_custom_domains_domain ON custom_domains(domain);
    `);

    await client.query(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.table_constraints
          WHERE table_name = 'config_events' AND constraint_name = 'config_events_kind_check'
        ) THEN
          ALTER TABLE config_events DROP CONSTRAINT config_events_kind_check;
        END IF;
        ALTER TABLE config_events
          ADD CONSTRAINT config_events_kind_check CHECK (kind IN (10002, 10063, 30405, 30406));

        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns 
          WHERE table_name = 'message_events' AND column_name = 'is_read'
        ) THEN
          ALTER TABLE message_events ADD COLUMN is_read BOOLEAN DEFAULT FALSE;
          CREATE INDEX IF NOT EXISTS idx_message_events_is_read ON message_events(is_read);
        END IF;
        
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns 
          WHERE table_name = 'message_events' AND column_name = 'order_status'
        ) THEN
          ALTER TABLE message_events ADD COLUMN order_status TEXT DEFAULT NULL;
        END IF;
        
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns 
          WHERE table_name = 'message_events' AND column_name = 'order_id'
        ) THEN
          ALTER TABLE message_events ADD COLUMN order_id TEXT DEFAULT NULL;
          CREATE INDEX IF NOT EXISTS idx_message_events_order_id ON message_events(order_id);
        END IF;
      END $$;
    `);

    await ensureFailedRelayPublishesTable(client);

    tablesInitialized = true;
    initializingTables = false;
  } catch (error) {
    console.error("Failed to initialize tables:", error);
    initializingTables = false;
    throw error;
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function closeDbPool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
