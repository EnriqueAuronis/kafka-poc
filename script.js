let globalCounter = 0; 
function createAccessItem(index) {
  return {
    accessCounter: 100 + index,
    accessDate: new Date().toISOString(),
    balanceNew: 50.0 + Math.random() * 10,
    balanceOld: 60.0 + Math.random() * 10,
    deviceId: `DEV_${index}`,
    discountAmount: 10.5,
    discountConcept: "Some Discount",
    latitude: 19.4326 + Math.random() * 0.01,
    longitude: -99.1332 + Math.random() * 0.01,
    passengers: 3,
    paymentCompany: "MyPay",
    paymentCompanyId: "PC123",
    paymentMethod: "Credit",
    paymentMethodId: "PM456",
    qrVersion: "v2",
    subtotal: 45.0 + Math.random() * 10,
    ticketId: `TIC_${1000 + index}`,
    total: 50.0 + Math.random() * 10,
    userCategory: "Adult",
    userId: `USR_${index}`,
    userProfile: "Basic",
    userTitle: "Mr.",
    no_bank: "N/A",
    bank: "BankXYZ",
    no_bank_flag: false,
    subscription: "Monthly"
  };
}

async function sendRequest(payload) {
  const url = "http://localhost:3000/app/sendAccess";

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify([payload])
    });

    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }

    const data = await response.json();
    console.log(`Response OK for ticketId: ${payload.ticketId}`, data);
  } catch (error) {
    console.error("Error sending request:", error);
  }
}

function main() {
  const REQUESTS_PER_SECOND = 1800;

  setInterval(async () => {
    const promises = [];

    for (let i = 0; i < REQUESTS_PER_SECOND; i++) {
      const item = createAccessItem(globalCounter++);
      promises.push(sendRequest(item));
    }

    Promise.all(promises).catch((err) => {
      console.error("Error in batch:", err);
    });
  }, 1000);

  console.log(`Started sending ${REQUESTS_PER_SECOND} events per second...`);
}

// Run
main();
