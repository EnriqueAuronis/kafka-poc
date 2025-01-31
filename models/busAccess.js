const pool = require('../db');

async function insertBusAccess(content) {
  const queryText = `
    INSERT INTO bus_access (
      access_counter,
      access_date,
      balance_new,
      balance_old,
      device_id,
      discount_amount,
      discount_concept,
      latitude,
      longitude,
      passengers,
      payment_company,
      payment_company_id,
      payment_method,
      payment_method_id,
      qr_version,
      subtotal,
      ticket_id,
      total,
      user_category,
      user_id,
      user_profile,
      user_title,
      no_bank,
      bank,
      no_bank_flag,
      subscription
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8,
      $9, $10, $11, $12, $13, $14, $15,
      $16, $17, $18, $19, $20, $21, $22,
      $23, $24, $25, $26
    )
    RETURNING id
  `;

  const values = [
    content.accessCounter,
    content.accessDate,
    content.balanceNew,
    content.balanceOld,
    content.deviceId,
    content.discountAmount,
    content.discountConcept,
    content.latitude,
    content.longitude,
    content.passengers,
    content.paymentCompany,
    content.paymentCompanyId,
    content.paymentMethod,
    content.paymentMethodId,
    content.qrVersion,
    content.subtotal,
    content.ticketId,
    content.total,
    content.userCategory,
    content.userId,
    content.userProfile,
    content.userTitle,
    content.no_bank,
    content.bank,
    content.no_bank_flag,
    content.subscription
  ];

  try {
    const result = await pool.query(queryText, values);
    if (result.rows && result.rows.length > 0) {
      console.log('Inserted bus_access row with id:', result.rows[0].id);
      return true;
    }
    return false;
  } catch (err) {
    console.error('Error inserting bus_access record:', err);
    return false;
  }
}

module.exports = { insertBusAccess };
