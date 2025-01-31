const pool = require('./index');

const createBusAccessTable = async () => {
  const query = `
    CREATE TABLE IF NOT EXISTS bus_access (
      id SERIAL PRIMARY KEY,
      access_counter       INTEGER,
      access_date         TIMESTAMPTZ,
      balance_new         NUMERIC(14, 2),
      balance_old         NUMERIC(14, 2),
      creation_date       TIMESTAMPTZ,
      device_id           TEXT,
      discount_amount     NUMERIC(14, 2),
      discount_concept    TEXT,
      latitude            DOUBLE PRECISION,
      longitude           DOUBLE PRECISION,
      passengers          INTEGER,
      payment_company     TEXT,
      payment_company_id  TEXT,
      payment_method      TEXT,
      payment_method_id   TEXT,
      qr_version          TEXT,
      subtotal            NUMERIC(14, 2),
      ticket_id           TEXT,
      total               NUMERIC(14, 2),
      user_category       TEXT,
      user_id             TEXT,
      user_profile        TEXT,
      user_title          TEXT,
      no_bank             TEXT,
      bank                TEXT,
      no_bank_flag        BOOLEAN,
      subscription        TEXT,
      created_at          TIMESTAMPTZ DEFAULT NOW()
    );
  `;
  await pool.query(query);
};

module.exports = { createBusAccessTable };
