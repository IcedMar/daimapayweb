require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const africastalking = require('africastalking')({
  apiKey: process.env.AT_API_KEY,
  username: process.env.AT_USERNAME,
});

// --- Firestore Initialization with Firebase Admin SDK ---
const admin = require('firebase-admin');
const serviceAccount = JSON.parse(process.env.GCP_SERVICE_ACCOUNT_KEY_JSON);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  projectId: process.env.GCP_PROJECT_ID, // Ensure this is also in your .env
});

const firestore = admin.firestore(); // Use admin.firestore()
// --- End Firestore Initialization ---

const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

const txCollection = firestore.collection('transactions');

const corsOptions = {
  origin: 'https://daima-pay-portal.onrender.com', // Your frontend origin
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions));
app.options('/*splat', cors(corsOptions)); // Handle preflight requests for all routes
app.use(bodyParser.json());

let cachedAirtimeToken = null;
let tokenExpiryTimestamp = 0;

// IMPORTANT: URL for your Analytics/Float Server's float deduction endpoint
const ANALYTICS_SERVER_FLOAT_DEDUCTION_URL = process.env.ANALYTICS_SERVER_URL || 'http://localhost:5002/api/process-airtime-purchase';


// Carrier detection helper
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

// Safaricom dealer token acquisition and caching
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

// Send Safaricom dealer airtime
async function sendSafaricomAirtime(receiverNumber, amount) {
  const token = await getCachedAirtimeToken();
  const normalizedReceiver = normalizeReceiverPhoneNumber(receiverNumber); // Ensure format expected by API
  const adjustedAmount = amount * 100; // Convert to cents if API expects it

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

// --- API Endpoints ---

// STK Push Payment Initiation
app.post('/pay', async (req, res) => {
  const { topupNumber, amount, mpesaNumber } = req.body;

  if (!topupNumber || !amount || !mpesaNumber) {
    return res.status(400).json({ error: 'Missing fields! All fields are required.' });
  }

  // Basic validation for amount
  if (isNaN(amount) || parseFloat(amount) <= 0) {
      return res.status(400).json({ error: 'Invalid amount. Amount must be a positive number.' });
  }

  const carrier = detectCarrier(topupNumber);
  console.log(`📡 Detected Carrier for ${topupNumber}: ${carrier}`);

  if (carrier === 'Unknown') {
    return res.status(400).json({ error: 'Unsupported carrier prefix. Please check the top-up number.' });
  }

  try {
    // Initiate M-Pesa STK push
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
      PartyB: process.env.TILL_SHORTCODE, // Your Paybill/Till number
      PhoneNumber: mpesaNumber,
      CallBackURL: `${process.env.BASE_URL}/stk-callback`, // Your callback URL
      AccountReference: 'DaimaPayAirtime', // Unique reference for the transaction
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

    console.log('✅ STK Push Initiated Response:', stkResponse.data);

    // Save initial PENDING transaction to Firestore
    await txCollection.doc(stkResponse.data.CheckoutRequestID).set({
      topupNumber,
      amount: parseFloat(amount), // Store as number for better querying
      payer: mpesaNumber,
      status: 'PENDING',
      carrier,
      // Use Firestore's server timestamp for consistency and query efficiency
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      mpesaRequestId: stkResponse.data.CustomerMessage || stkResponse.data.ResponseDescription,
      merchantRequestId: stkResponse.data.MerchantRequestID,
      checkoutRequestID: stkResponse.data.CheckoutRequestID, // Store for clarity
    });

    res.json({
      message: `STK push initiated for ${carrier}. Please complete the payment on your phone.`,
      CheckoutRequestID: stkResponse.data.CheckoutRequestID, // Return only this unique ID
    });

  } catch (err) {
    console.error('❌ STK Push Error:', err.response ? err.response.data : err.message);
    let errorMessage = 'STK push failed. An unexpected error occurred.';
    if (err.response && err.response.data && err.response.data.errorMessage) {
        errorMessage = err.response.data.errorMessage;
    } else if (err.message) {
        errorMessage = err.message;
    }
    res.status(500).json({ error: errorMessage });
  }
});

// M-Pesa STK Callback Handler
app.post('/stk-callback', async (req, res) => {
  const callback = req.body;

  console.log('📞 Received STK Callback:', JSON.stringify(callback));

  const resultCode = callback.Body.stkCallback.ResultCode;
  const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID;

  const txDocRef = txCollection.doc(checkoutRequestID);
  const txDoc = await txDocRef.get();

  if (!txDoc.exists) {
    console.error('❌ No matching transaction for CheckoutRequestID in Firestore:', checkoutRequestID);
    return res.json({ resultCode: 0, resultDesc: 'No matching transaction found locally for this callback.' });
  }

  const txData = txDoc.data();
  const { topupNumber, amount, carrier, payer } = txData;

  let transactionStatus = 'UNKNOWN';
  let updateData = {};

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

      updateData = {
        status: transactionStatus,
        completedAt: admin.firestore.FieldValue.serverTimestamp(), 
        airtimeResult: airtimeResult,
        // Extract M-Pesa specific details from callback for successful payments
        mpesaReceiptNumber: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'MpesaReceiptNumber')?.Value,
        balance: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'Balance')?.Value,
        transactionDate: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'TransactionDate')?.Value, // M-Pesa's transaction date string
        phoneNumberUsedForPayment: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'PhoneNumber')?.Value,
      };

      // --- Call the Analytics/Float Server to deduct float ---
      const floatDeductionPayload = {
        amount: parseFloat(amount),
        status: 'SUCCESS',
        telco: carrier,
        transactionId: checkoutRequestID,
        txCode: airtimeResult.txCode || (airtimeResult.entries && airtimeResult.entries[0] && airtimeResult.entries[0].status === 'Sent' ? airtimeResult.entries[0].status : 'N/A'), // Get AT status if available
        purchaserPhone: payer,
        topupNumber: topupNumber,
        mpesaReceiptNumber: updateData.mpesaReceiptNumber, // Pass M-Pesa receipt
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
        // Log critical error for manual review. This doesn't block STK callback.
      }

    } catch (err) {
      console.error('❌ Airtime send failed:', err.response ? err.response.data : err.message);
      transactionStatus = 'FAILED_AIRTIME_SEND'; // More specific status
      updateData = {
        status: transactionStatus,
        error: err.toString(),
        completedAt: admin.firestore.FieldValue.serverTimestamp(), // Server timestamp for completion
        // Still save M-Pesa details even if airtime failed
        mpesaReceiptNumber: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'MpesaReceiptNumber')?.Value,
        balance: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'Balance')?.Value,
        transactionDate: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'TransactionDate')?.Value,
        phoneNumberUsedForPayment: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'PhoneNumber')?.Value,
      };
    }
  } else {
    // M-Pesa payment failed or was cancelled by user
    console.log(`❌ Payment failed for ${checkoutRequestID}. ResultCode: ${resultCode}, Desc: ${callback.Body.stkCallback.ResultDesc}`);
    transactionStatus = 'FAILED_PAYMENT';
    updateData = {
      status: transactionStatus,
      resultCode: resultCode,
      resultDesc: callback.Body.stkCallback.ResultDesc,
      completedAt: admin.firestore.FieldValue.serverTimestamp(), // Server timestamp for completion
      mpesaRequestId: txData.mpesaRequestId, // Use original stored value
      merchantRequestId: txData.merchantRequestId, // Use original stored value
    };
  }

  // Update the Firestore document with the final status and details
  await txDocRef.update(updateData);

  // Always respond to M-Pesa to acknowledge callback receipt
  res.json({ resultCode: 0, resultDesc: 'Callback received and processed by DaimaPay server.' });
});


// New endpoint for frontend to poll transaction status
app.get('/transaction-status/:checkoutRequestID', async (req, res) => {
    const { checkoutRequestID } = req.params;
    try {
        const txDoc = await txCollection.doc(checkoutRequestID).get();

        if (!txDoc.exists) {
            console.warn(`Attempted to fetch status for non-existent CheckoutRequestID: ${checkoutRequestID}`);
            return res.status(404).json({ error: 'Transaction not found.' });
        }

        const data = txDoc.data();
        // Return only necessary status fields to the client.
        // Convert Firestore Timestamps to ISO strings for client consumption if needed.
        const createdAtISO = data.createdAt ? data.createdAt.toDate().toISOString() : null;
        const completedAtISO = data.completedAt ? data.completedAt.toDate().toISOString() : null;

        res.json({
            status: data.status,
            completedAt: completedAtISO,
            createdAt: createdAtISO,
            mpesaReceiptNumber: data.mpesaReceiptNumber || null,
            // You can add more non-sensitive fields if your frontend needs them
            amount: data.amount,
            topupNumber: data.topupNumber,
            payer: data.payer,
            carrier: data.carrier
        });
    } catch (error) {
        console.error('Error fetching transaction status for:', checkoutRequestID, error);
        res.status(500).json({ error: 'Failed to fetch transaction status.' });
    }
});


// Default route for health check
app.get('/', (req, res) => {
  res.send('DaimaPay backend is live ✅');
});

// Start the server
app.listen(PORT, () => {
  console.log(`🚀 Web server running on port ${PORT}`);
});
