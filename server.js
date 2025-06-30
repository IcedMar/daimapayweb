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

const firestore = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.GCP_KEY_FILE,
});

const txCollection = firestore.collection('transactions');

const corsOptions = {
  origin: 'https://daima-pay-portal.onrender.com',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions));
app.options('/*splat', cors(corsOptions));
app.use(bodyParser.json());

// Carrier detection helper
function detectCarrier(phoneNumber) {
  const normalized = phoneNumber.replace(/^(\+254|254)/, '0').trim();
  const prefix3 = normalized.substring(1, 4); // after '0'
  const safaricom = new Set([
    ...range(110, 119),
    ...range(701, 709),
    ...range(710, 719),
    ...range(720, 729),
    '740',
    '741',
    '742',
    '743',
    '745',
    '746',
    '748',
    '757',
    '758',
    '759',
    '768',
    ...range(790, 799)
  ]);
  const airtel = new Set([
    '100',
    '101',
    '102',
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

// Payment handler
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
    // 1. Initiate M-Pesa STK push as usual
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
    });

    res.json({
      message: `STK push initiated for ${carrier}.`,
      CheckoutRequestID: stkResponse.data.CheckoutRequestID,
      transID: stkResponse.data.CheckoutRequestID,
    });

  } catch (err) {
    console.error(err.response ? err.response.data : err);
    res.status(500).json({ error: 'STK push failed' });
  }
});

// Callback handler
app.post('/stk-callback', async (req, res) => {
  const callback = req.body;

  console.log('📞 Received STK Callback:', JSON.stringify(callback));

  const resultCode = callback.Body.stkCallback.ResultCode;
  const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID;

  const txDoc = await txCollection.doc(checkoutRequestID).get();

  if (!txDoc.exists) {
    console.error('❌ No matching transaction for:', checkoutRequestID);
    return res.json({ resultCode: 0, resultDesc: 'No matching transaction' });
  }

  const txData = txDoc.data();
  const { topupNumber, amount, carrier } = txData;

  if (resultCode === 0) {
    console.log(`✅ Payment success. Carrier: ${carrier}`);

    try {
      if (carrier === 'Safaricom') {
        // TODO: integrate your dealer’s Safaricom airtime API here
        console.log(`🚀 Loading Safaricom airtime to ${topupNumber}`);
        // Example: await axios.post('https://dealer-api.example/load', { topupNumber, amount });

      } else if (carrier === 'Airtel' || carrier === 'Telkom') {
        const response = await africastalking.AIRTIME.send({
          recipients: [{ phoneNumber: topupNumber, amount: `KES ${amount}` }],
        });
        console.log('✅ Airtel/Telkom Airtime sent:', response);
      }

      await txCollection.doc(checkoutRequestID).update({
        status: 'COMPLETED',
        completedAt: new Date().toISOString(),
      });

    } catch (err) {
      console.error('❌ Airtime send failed:', err);
      await txCollection.doc(checkoutRequestID).update({
        status: 'FAILED_AIRTIME',
        error: err.toString(),
      });
    }
  } else {
    console.log('❌ Payment failed or cancelled.');
    await txCollection.doc(checkoutRequestID).update({
      status: 'FAILED_PAYMENT',
    });
  }

  res.json({ resultCode: 0, resultDesc: 'Callback received' });
});

app.get('/', (req, res) => {
  res.send('DaimaPay backend is live ✅');
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
