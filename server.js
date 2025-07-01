require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios'); 
const africastalking = require('africastalking')({
  apiKey: process.env.AT_API_KEY,
  username: process.env.AT_USERNAME,
});
const { Firestore } = require('@google-cloud/firestore'); 
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Initialize Firestore for this server (frontend server)
const firestore = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.GCP_KEY_FILE,
});

const txCollection = firestore.collection('transactions');

const corsOptions = {
  origin: 'https://daima-pay-portal.onrender.com', // Your frontend origin
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions));
app.options('/*splat', cors(corsOptions));
app.use(bodyParser.json());

let cachedAirtimeToken = null;
let tokenExpiryTimestamp = 0;

// --- IMPORTANT: URL for your Analytics/Float Server's float deduction endpoint ---
// Make sure this matches where your analytics server (the first server.js we modified) is running.
// In production, this would be a public URL, e.g., 'https://your-analytics-api.com/api/process-airtime-purchase'
const ANALYTICS_SERVER_FLOAT_DEDUCTION_URL = process.env.ANALYTICS_SERVER_URL || 'http://localhost:5002/api/process-airtime-purchase';


// Carrier detection helper (unchanged)
function detectCarrier(phoneNumber) {
  const normalized = phoneNumber.replace(/^(\+254|254)/, '0').trim();
  const prefix3 = normalized.substring(1, 4); // after '0'
  const safaricom = new Set([
    ...range(110, 119),
    ...range(701, 709),
    ...range(710, 719),
    ...range(720, 729),
    '740', '741', '742', '743', '745', '746', '748',
    '757', '758', '759', '768',
    ...range(790, 799)
  ]);
  const airtel = new Set([
    '100', '101', '102',
    ...range(730, 739),
    ...range(750, 756),
    ...range(780, 789)
  ]);
  const telkom = new Set(range(770, 779));

  if (safaricom.has(prefix3)) return 'Safaricom';
  if (airtel.has(prefix3)) return 'Airtel';
  if (telkom.has(prefix3)) return 'Telkom';
  return 'Unknown';

  function range(a, b) {
    const arr = [];
    for (let i = a; i <= b; i++) arr.push(String(i));
    return arr;
  }
}

// ✅ Safaricom dealer token (unchanged)
async function getCachedAirtimeToken() {
  const now = Date.now();
  if (cachedAirtimeToken && now < tokenExpiryTimestamp) {
    console.log('🔑 Using cached dealer token');
    return cachedAirtimeToken;
  }
  const auth = Buffer.from(`${process.env.MPESA_AIRTIME_KEY}:${process.env.MPESA_AIRTIME_SECRET}`).toString('base64');
  const response = await axios.post(
    process.env.MPESA_GRANT_URL,
    {},
    {
      headers: {
        Authorization: `Basic ${auth}`,
        'Content-Type': 'application/json',
      },
    }
  );
  const token = response.data.access_token;
  cachedAirtimeToken = token;
  tokenExpiryTimestamp = now + 3599 * 1000;
  return token;
}

function normalizeReceiverPhoneNumber(num) {
  return num.replace(/^(\+254|254)/, '0').trim();
}

// ✅ Send Safaricom dealer airtime (unchanged)
async function sendSafaricomAirtime(receiverNumber, amount) {
  const token = await getCachedAirtimeToken();
  const normalizedReceiver = normalizeReceiverPhoneNumber(receiverNumber);
  const adjustedAmount = amount * 100; 

  const body = {
    senderMsisdn: process.env.DEALER_SENDER_MSISDN,
    amount: adjustedAmount,
    servicePin: process.env.DEALER_SERVICE_PIN, 
    receiverMsisdn: normalizedReceiver,
  };

  const response = await axios.post(
    process.env.MPESA_AIRTIME_URL,
    body,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    }
  );

  console.log('✅ Safaricom dealer airtime API response:', response.data);
  return response.data;
}

// Payment handler (unchanged, just added console log for clarity)
app.post('/pay', async (req, res) => {
  const { topupNumber, amount, mpesaNumber } = req.body;

  if (!topupNumber || !amount || !mpesaNumber) {
    return res.status(400).json({ error: 'Missing fields!' });
  }

  const carrier = detectCarrier(topupNumber);
  console.log(`📡 Detected Carrier: ${carrier}`);

  if (carrier === 'Unknown') {
    return res.status(400).json({ error: 'Unsupported carrier prefix.' });
  }

  try {
    // Initiate M-Pesa STK push as usual
    const auth = Buffer.from(
      `${process.env.DARAJA_CONSUMER_KEY}:${process.env.DARAJA_CONSUMER_SECRET}`
    ).toString('base64');

    const authResponse = await axios.get(
      'https://api.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials',
      {
        headers: { Authorization: `Basic ${auth}` },
      }
    );

    const access_token = authResponse.data.access_token;

    const timestamp = new Date().toISOString().replace(/[-T:.Z]/g, '').slice(0, 14);
    const password = Buffer.from(
      `${process.env.BUSINESS_SHORTCODE}${process.env.DARAJA_PASSKEY}${timestamp}`
    ).toString('base64');

    const stkPushPayload = {
      BusinessShortCode: process.env.BUSINESS_SHORTCODE,
      Password: password,
      Timestamp: timestamp,
      TransactionType: 'CustomerPayBillOnline',
      Amount: amount,
      PartyA: mpesaNumber,
      PartyB: process.env.TILL_SHORTCODE,
      PhoneNumber: mpesaNumber,
      CallBackURL: `${process.env.BASE_URL}/stk-callback`,
      AccountReference: 'DaimaPay',
      TransactionDesc: 'Airtime Purchase',
    };

    const stkResponse = await axios.post(
      'https://api.safaricom.co.ke/mpesa/stkpush/v1/processrequest',
      stkPushPayload,
      {
        headers: {
          Authorization: `Bearer ${access_token}`,
        },
      }
    );

    console.log('✅ STK Push Response:', stkResponse.data);

    await txCollection.doc(stkResponse.data.CheckoutRequestID).set({
      topupNumber,
      amount,
      payer: mpesaNumber,
      status: 'PENDING',
      carrier,
      createdAt: new Date().toISOString(),
      // Store the MpesaRequestId for potential future use or matching
      mpesaRequestId: stkResponse.data.CustomerMessage || stkResponse.data.ResponseDescription, 
      merchantRequestId: stkResponse.data.MerchantRequestID,
    });

    res.json({
      message: `STK push initiated for ${carrier}.`,
      CheckoutRequestID: stkResponse.data.CheckoutRequestID,
      transID: stkResponse.data.CheckoutRequestID, 
    });

  } catch (err) {
    console.error('❌ STK Push Error:', err.response ? err.response.data : err.message);
    res.status(500).json({ error: 'STK push failed. ' + (err.response ? err.response.data.errorMessage : err.message) });
  }
});

// Callback handler - MODIFIED TO DEDUCT FLOAT
app.post('/stk-callback', async (req, res) => {
  const callback = req.body;

  console.log('📞 Received STK Callback:', JSON.stringify(callback));

  const resultCode = callback.Body.stkCallback.ResultCode;
  const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID;

  const txDoc = await txCollection.doc(checkoutRequestID).get();

  if (!txDoc.exists) {
    console.error('❌ No matching transaction for CheckoutRequestID:', checkoutRequestID);
    return res.json({ resultCode: 0, resultDesc: 'No matching transaction' });
  }

  const txData = txDoc.data();
  const { topupNumber, amount, carrier, payer, createdAt, mpesaRequestId, merchantRequestId } = txData; // Extract more data

  let transactionStatus = 'UNKNOWN'; 

  if (resultCode === 0) {
    // M-Pesa payment was successful
    let airtimeResult;
    try {
      if (carrier === 'Safaricom') {
        airtimeResult = await sendSafaricomAirtime(topupNumber, amount);
      } else {
        // For Airtel/Telkom via Africa's Talking
        airtimeResult = await africastalking.AIRTIME.send({
          recipients: [{ phoneNumber: topupNumber, amount: `KES ${amount}` }],
        });
      }

      console.log('✅ Airtime sent:', airtimeResult);
      transactionStatus = 'COMPLETED'; 
      
      await txCollection.doc(checkoutRequestID).update({
        status: transactionStatus,
        completedAt: new Date().toISOString(),
        airtimeResult: airtimeResult,
        // Optional: Save M-Pesa specific details from callback
        mpesaReceiptNumber: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'MpesaReceiptNumber')?.Value,
        balance: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'Balance')?.Value,
        transactionDate: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'TransactionDate')?.Value,
        phoneNumberUsedForPayment: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'PhoneNumber')?.Value,
      });

      // --- NEW: Call the Analytics/Float Server to deduct float ---
      const floatDeductionPayload = {
        amount: amount,
        status: 'SUCCESS', 
        telco: carrier, 
        transactionId: checkoutRequestID, 
        // You might send more data for logging on the analytics server, e.g.:
        txCode: airtimeResult.txCode || 'N/A', 
        purchaserPhone: payer,
        topupNumber: topupNumber,
      };

      console.log(`📞 Attempting to deduct float for ${checkoutRequestID} from Analytics Server.`);
      try {
        const floatResponse = await axios.post(
          ANALYTICS_SERVER_FLOAT_DEDUCTION_URL,
          floatDeductionPayload
        );
        console.log('✅ Float deduction response from Analytics Server:', floatResponse.data);
      } catch (floatErr) {
        console.error('❌ Failed to deduct float on Analytics Server:', floatErr.response ? floatErr.response.data : floatErr.message);
        // IMPORTANT: If float deduction fails, you might want to log this
        // to a critical error log, trigger an alert, or even implement a
        // retry mechanism. This doesn't block the STK callback response.
      }

    } catch (err) {
      console.error('❌ Airtime send failed:', err.response ? err.response.data : err.message);
      transactionStatus = 'FAILED_AIRTIME';
      await txCollection.doc(checkoutRequestID).update({
        status: transactionStatus,
        error: err.toString(),
        completedAt: new Date().toISOString(),
      });
    }
  } else {
    // M-Pesa payment failed or was cancelled
    console.log(`❌ Payment failed for ${checkoutRequestID}. ResultCode: ${resultCode}`);
    transactionStatus = 'FAILED_PAYMENT';
    await txCollection.doc(checkoutRequestID).update({
      status: transactionStatus,
      resultCode: resultCode,
      resultDesc: callback.Body.stkCallback.ResultDesc,
      completedAt: new Date().toISOString(),
      // Optional: Save M-Pesa specific details from callback for failed payment
      mpesaRequestId: mpesaRequestId, 
      merchantRequestId: merchantRequestId,
    });
  }

  // Always respond to M-Pesa to acknowledge callback receipt
  res.json({ resultCode: 0, resultDesc: 'Callback received and processed' });
});

// Default route for serving static files (assuming you have an 'public' or 'dist' folder)
// If your frontend is served by a different mechanism (e.g., a React dev server, Nginx),
// you might remove this. If it's pure HTML/JS served by this Express, keep it.
// Example: app.use(express.static('public'));

app.get('/', (req, res) => {
  res.send('DaimaPay backend is live ✅');
});

app.listen(PORT, () => {
  console.log(`🚀 Web server running on port ${PORT}`);
});
